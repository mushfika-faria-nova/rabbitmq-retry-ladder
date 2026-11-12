package io.github.mushfikafarianova.retryladder;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.MessagePropertiesBuilder;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import com.rabbitmq.client.Channel;

import io.github.mushfikafarianova.retryladder.Decision.Park;
import io.github.mushfikafarianova.retryladder.Decision.Retry;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The one bean apps use. Call {@link #handle} from a listener in manual ack mode (decision 3):
 * it runs the work, then acks, retries or parks the message. The original is acked only after
 * its retry copy or parked copy is confirmed by RabbitMQ (decision 6).
 */
public class RetryLadder {

    private static final Log log = LogFactory.getLog(RetryLadder.class);

    private final Map<String, StopRule> rules = new HashMap<>();
    private final Map<String, QueuePlan> plans = new HashMap<>();
    private final Set<String> warnedQueues = ConcurrentHashMap.newKeySet();
    private final Topology topology;
    private final RabbitTemplate template;
    private final Duration confirmTimeout;
    private final MeterRegistry meters;

    RetryLadder(RetryPlans retryPlans, Topology topology, RabbitTemplate template, Duration confirmTimeout,
            MeterRegistry meters, Clock clock) {
        for (QueuePlan plan : retryPlans.plans()) {
            plans.put(plan.queue(), plan);
            rules.put(plan.queue(), new StopRule(plan, retryPlans.nonRetryable(), clock));
        }
        this.topology = topology;
        this.template = template;
        this.confirmTimeout = confirmTimeout;
        this.meters = meters;
    }

    /**
     * Runs the work for one message. On success the message is acked. On failure it is retried after a
     * wait, or parked. If the work throws an {@link Error}, the message is rejected so RabbitMQ parks it
     * (decision 11). Throws only if talking to RabbitMQ fails, or the Error means the JVM itself is broken.
     */
    public void handle(Message message, Channel channel, MessageWork work) throws IOException {
        long tag = message.getMessageProperties().getDeliveryTag();
        warnOnceIfNotManaged(message.getMessageProperties().getConsumerQueue());
        // The work may change the message; decide and copy from its properties as they arrived (R-4).
        MessageProperties arrived = MessagePropertiesBuilder.fromClonedProperties(message.getMessageProperties())
                .build();
        try {
            work.process(message);
        } catch (Exception error) {
            onFailure(new Message(message.getBody(), arrived), channel, tag, error);
            return;
        } catch (Error error) {
            onError(arrived, channel, tag, error);
            return;
        }
        channel.basicAck(tag, false);
    }

    /** A queue someone forgot to list shows up on its first message, not only when a message fails. */
    private void warnOnceIfNotManaged(String queue) {
        if (queue != null && !rules.containsKey(queue) && warnedQueues.add(queue)) {
            log.error("Queue '" + queue + "' isn't under retry-ladder.queues, so its failed messages can't be "
                    + "retried or parked: they are rejected, and lost if the queue has no dead-letter setting. "
                    + "Add the queue under retry-ladder.queues.");
        }
    }

    private void onFailure(Message message, Channel channel, long tag, Exception error) throws IOException {
        MessageProperties props = message.getMessageProperties();
        String queue = props.getConsumerQueue();
        String messageId = props.getMessageId();
        StopRule rule = queue == null ? null : rules.get(queue);
        if (rule == null) {
            log.error("Message " + messageId + " came from queue '" + queue + "', which retry-ladder doesn't manage; "
                    + "rejecting it. Add the queue under retry-ladder.queues.");
            channel.basicReject(tag, false);
            return;
        }
        if (StopRule.resetIfReplayed(props.getHeaders())) {
            log.info("Message " + messageId + " on queue '" + queue + "' was replayed from "
                    + Topology.parkingQueue(queue) + "; starting a fresh retry history");
        }
        Decision decision = rule.decide(props.getHeaders(), error);
        Message copy = MessageBuilder.fromClonedMessage(message).copyHeaders(decision.headers()).build();
        // Spring clears the delivery mode on receive; send the copy with the one the original had (R-4).
        copy.getMessageProperties().setDeliveryMode(props.getReceivedDeliveryMode());

        String failure = switch (decision) {
            case Retry r -> store(topology.retryExchange(), topology.delayRoutingKey(r.delay()), copy);
            case Park p -> store(topology.parkingExchange(), Topology.parkingQueue(queue), copy);
        };
        if (failure != null) {
            meters.counter("retry.ladder.park.failed", "queue", queue, "cause", failure).increment();
            log.error("Could not store message " + messageId + " from queue '" + queue + "' (" + failure
                    + "); rejecting it so RabbitMQ parks it in " + Topology.parkingQueue(queue)
                    + ". retryCount=" + decision.headers().get(Headers.RETRY_COUNT));
            parkByReject(channel, tag);
            return;
        }
        switch (decision) {
            case Retry r -> logRetry(queue, messageId, r);
            case Park p -> {
                meters.counter("retry.ladder.exhausted", "queue", queue, "reason", p.reason().value()).increment();
                log.warn("Message " + messageId + " parked in " + Topology.parkingQueue(queue) + ": queue=" + queue
                        + " retryCount=" + p.headers().get(Headers.RETRY_COUNT) + " reason=" + p.reason().value());
            }
        }
        channel.basicAck(tag, false);
    }

    /**
     * The work threw an Error. A retry could hit it again, and letting it reach Spring stops the listener
     * (and by default the app). So park it by rejecting it (decisions 11 and 13).
     */
    private void onError(MessageProperties arrived, Channel channel, long tag, Error error) throws IOException {
        String queue = String.valueOf(arrived.getConsumerQueue());
        meters.counter("retry.ladder.work.errors", "queue", queue).increment();
        String where = rules.containsKey(queue) ? "so RabbitMQ parks it in " + Topology.parkingQueue(queue)
                : "(retry-ladder doesn't manage this queue)";
        log.error("Message " + arrived.getMessageId() + " from queue '" + queue + "': the work threw "
                + error.getClass().getName() + "; rejecting it " + where, error);
        try {
            parkByReject(channel, tag);
        } finally {
            // Out of memory and the like: the JVM itself is broken, so let Spring handle it as it does today.
            if (error instanceof VirtualMachineError && !(error instanceof StackOverflowError)) {
                throw error;
            }
        }
    }

    /**
     * Rejects without requeue. On a managed queue, the dead-letter setting Topology gave it then moves the
     * message to its parking queue: the library's safety net when it can't store a copy itself.
     */
    private static void parkByReject(Channel channel, long tag) throws IOException {
        channel.basicReject(tag, false);
    }

    /** Publishes and waits for RabbitMQ's confirm. Returns null when stored, or the reason it isn't proven stored. */
    private String store(String exchange, String routingKey, Message copy) {
        CorrelationData correlation = new CorrelationData();
        try {
            template.send(exchange, routingKey, copy, correlation);
            CorrelationData.Confirm confirm = correlation.getFuture()
                    .get(confirmTimeout.toMillis(), TimeUnit.MILLISECONDS);
            // Spring sets the returned message before it completes the confirm, so this check is safe.
            if (correlation.getReturned() != null) {
                return "returned";
            }
            return confirm.ack() ? null : "nacked";
        } catch (TimeoutException ex) {
            return "timeout";
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return "interrupted";
        } catch (ExecutionException | RuntimeException ex) {
            // Anything else that goes wrong while sending: not proven stored, so the caller parks it.
            return "error";
        }
    }

    /** One line per retry with its whole timeline (R-7). Never the message body. */
    private void logRetry(String queue, String messageId, Retry retry) {
        meters.counter("retry.ladder.scheduled", "queue", queue, "tier", Long.toString(retry.delay().toMillis()))
                .increment();
        if (!log.isDebugEnabled()) {
            return;
        }
        // The time the retry was planned, as written in the headers, so the log line matches them.
        long now = (Long) retry.headers().get(Headers.LAST_RETRY_AT);
        Object first = retry.headers().get(Headers.FIRST_FAILED_AT);
        QueuePlan plan = plans.get(queue);
        String elapsed = "unknown";
        String timeLeft = "unknown";
        if (first instanceof Long f) {
            elapsed = QueuePlan.format(Duration.ofMillis(now - f));
            timeLeft = QueuePlan.format(plan.window().minusMillis(now - f).minus(retry.delay()));
        }
        log.debug("Retry scheduled: messageId=" + messageId + " queue=" + queue
                + " retry=" + retry.headers().get(Headers.RETRY_COUNT)
                + " wait=" + QueuePlan.format(retry.delay())
                + " elapsed=" + elapsed
                + " nextTryAt=" + Instant.ofEpochMilli(now).plus(retry.delay())
                + " timeLeft=" + timeLeft);
    }
}

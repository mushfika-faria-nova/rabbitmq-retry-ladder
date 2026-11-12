package io.github.mushfikafarianova.retryladder;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * The retry flow against a real RabbitMQ (R-1 to R-7). Each test uses its own name prefix and queue,
 * so tests don't share delay queues. Waits are in milliseconds so the suite stays fast.
 */
@Testcontainers
@ExtendWith(OutputCaptureExtension.class)
class RetryFlowTest {

    @Container
    static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:4.1-management");

    /** One delivery seen by the app's work: when it arrived and what it was. */
    record Delivery(long nanos, Message message) {
        long millisSince(Delivery earlier) {
            return TimeUnit.NANOSECONDS.toMillis(nanos - earlier.nanos);
        }
    }

    /** The app's work in these tests. Records every delivery; the behaviour decides success or failure. */
    static class TestWork {
        final BlockingQueue<Delivery> deliveries = new LinkedBlockingQueue<>();
        final List<Delivery> seen = new java.util.concurrent.CopyOnWriteArrayList<>();
        volatile Function<Message, Exception> behaviour = m -> new IllegalStateException("downstream is down");

        void process(Message m) throws Exception {
            Delivery d = new Delivery(System.nanoTime(), m);
            deliveries.add(d);
            seen.add(d);
            Exception failure = behaviour.apply(m);
            if (failure != null) {
                throw failure;
            }
        }

        Delivery next() throws InterruptedException {
            Delivery d = deliveries.poll(15, TimeUnit.SECONDS);
            assertThat(d).as("a delivery arrived").isNotNull();
            return d;
        }
    }

    static class BadPayload extends RuntimeException {
    }

    /** The app side: one listener per managed queue, in manual ack mode, calling RetryLadder.handle. */
    @Configuration(proxyBeanMethods = false)
    static class App {
        @Bean
        SimpleMessageListenerContainer listener(ConnectionFactory cf, RetryLadder ladder, RetryPlans plans,
                TestWork work) {
            SimpleMessageListenerContainer c = new SimpleMessageListenerContainer(cf);
            c.setQueueNames(plans.plans().stream().map(QueuePlan::queue).toArray(String[]::new));
            c.setAcknowledgeMode(AcknowledgeMode.MANUAL);
            c.setPrefetchCount(1);
            c.setMessageListener((ChannelAwareMessageListener) (m, ch) -> ladder.handle(m, ch, work::process));
            // Spring's default exits the JVM when an Error reaches the container; keep the test JVM alive.
            c.setJavaLangErrorHandler(e -> { });
            return c;
        }

        @Bean
        MeterRegistry meters() {
            return new SimpleMeterRegistry();
        }
    }

    static ApplicationContextRunner app(String prefix, TestWork work, String... settings) {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RabbitAutoConfiguration.class,
                        RetryLadderAutoConfiguration.class))
                .withUserConfiguration(App.class)
                .withBean(TestWork.class, () -> work)
                .withPropertyValues(
                        "spring.rabbitmq.host=" + RABBIT.getHost(),
                        "spring.rabbitmq.port=" + RABBIT.getAmqpPort(),
                        "spring.rabbitmq.username=" + RABBIT.getAdminUsername(),
                        "spring.rabbitmq.password=" + RABBIT.getAdminPassword(),
                        "retry-ladder.exchange-prefix=" + prefix,
                        "retry-ladder.initial-delay=200ms",
                        "retry-ladder.multiplier=2",
                        "retry-ladder.max-delay=800ms",
                        "retry-ladder.window=1m")
                .withPropertyValues(settings);
    }

    static Message message(String id, String body) {
        MessageProperties p = new MessageProperties();
        p.setMessageId(id);
        p.setContentType("application/json");
        p.setHeader("app-header", "kept");
        return new Message(body.getBytes(StandardCharsets.UTF_8), p);
    }

    static void assertUnchanged(Message got, Message sent) {
        assertThat(got.getBody()).isEqualTo(sent.getBody());
        MessageProperties p = got.getMessageProperties();
        assertThat(p.getMessageId()).isEqualTo(sent.getMessageProperties().getMessageId());
        assertThat(p.getContentType()).isEqualTo("application/json");
        assertThat((Object) p.getHeader("app-header")).isEqualTo("kept");
        assertThat(p.getReceivedDeliveryMode()).isEqualTo(sent.getMessageProperties().getDeliveryMode());
    }

    @Test
    void waitsGrowThenLevelOff() throws Exception { // R-1
        TestWork work = new TestWork();
        app("t1", work, "retry-ladder.queues.t1-orders.max-attempts=100").run(ctx -> {
            ctx.getBean(RabbitTemplate.class).send("", "t1-orders", message("m-1", "{}"));
            List<Delivery> d = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                d.add(work.next());
            }
            long[] expected = {200, 400, 800, 800, 800};
            for (int i = 0; i < expected.length; i++) {
                long gap = d.get(i + 1).millisSince(d.get(i));
                System.out.println("TIMING gap " + (i + 1) + ": " + gap + "ms (wait " + expected[i] + "ms)");
                assertThat(gap).as("gap before try %d", i + 2).isBetween(expected[i] - 20, expected[i] + 1_000);
            }
        });
    }

    @Test
    void aMessageOutOfTimeIsParkedUnchangedWithReasonWindow() throws Exception { // R-2, R-4, R-6
        TestWork work = new TestWork();
        // Waits 200, 400, 800ms; window 1s: tries at about 0, 200 and 600ms, then 600 + 800 is past 1s.
        app("t2", work, "retry-ladder.queues.t2-orders.window=1s").run(ctx -> {
            RabbitTemplate rabbit = ctx.getBean(RabbitTemplate.class);
            Message sent = message("m-2", "{\"order\":2}");
            rabbit.send("", "t2-orders", sent);
            Message parked = rabbit.receive("t2-orders.dlq", 15_000);
            assertThat(parked).as("parked message").isNotNull();
            assertUnchanged(parked, sent);
            assertThat((Object) parked.getMessageProperties().getHeader(Headers.EXHAUSTED_REASON)).isEqualTo("window");
            assertThat((Object) parked.getMessageProperties().getHeader(Headers.RETRY_COUNT)).isEqualTo(2);
            assertThat(work.deliveries).hasSize(3);
        });
    }

    @Test
    void anErrorThatNeverSucceedsIsParkedOnTheFirstTry() throws Exception { // R-3
        TestWork work = new TestWork();
        work.behaviour = m -> new RuntimeException("wrapped", new BadPayload());
        app("t3", work, "retry-ladder.queues.t3-orders.window=1m",
                "retry-ladder.non-retryable=" + BadPayload.class.getName()).run(ctx -> {
            RabbitTemplate rabbit = ctx.getBean(RabbitTemplate.class);
            rabbit.send("", "t3-orders", message("m-3", "{}"));
            Message parked = rabbit.receive("t3-orders.dlq", 15_000);
            assertThat(parked).isNotNull();
            assertThat((Object) parked.getMessageProperties().getHeader(Headers.EXHAUSTED_REASON))
                    .isEqualTo("non-retryable");
            assertThat(work.deliveries).hasSize(1);
        });
    }

    @Test
    void aRetryCopyThatCanNotBeStoredIsParkedNotLost() throws Exception { // R-4
        TestWork work = new TestWork();
        app("t4", work, "retry-ladder.queues.t4-orders.window=1m").run(ctx -> {
            RabbitTemplate rabbit = ctx.getBean(RabbitTemplate.class);
            rabbit.execute(ch -> null); // connect, so the queues are declared
            // Break the route to the first delay queue: the retry copy will come back unroutable.
            ctx.getBean(AmqpAdmin.class).removeBinding(new Binding("t4.delay.200", Binding.DestinationType.QUEUE,
                    "t4.retry", "delay.200", null));
            Message sent = message("m-4", "{}");
            rabbit.send("", "t4-orders", sent);
            Message parked = rabbit.receive("t4-orders.dlq", 15_000);
            assertThat(parked).as("original parked by the queue's dead-letter setting").isNotNull();
            assertUnchanged(parked, sent);
            assertThat(ctx.getBean(MeterRegistry.class)
                    .counter("retry.ladder.park.failed", "queue", "t4-orders", "cause", "returned").count())
                    .isEqualTo(1);
        });
    }

    @Test
    void aCrashBeforeTheAckLosesNothing() throws Exception { // R-4
        TestWork work = new TestWork();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        work.behaviour = m -> {
            if (!m.getMessageProperties().isRedelivered()) {
                started.countDown();
                try {
                    release.await(15, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return null; // the work itself succeeds
        };
        app("t5", work, "retry-ladder.queues.t5-orders.window=1m").run(ctx -> {
            RabbitTemplate rabbit = ctx.getBean(RabbitTemplate.class);
            rabbit.send("", "t5-orders", message("m-5", "{}"));
            assertThat(started.await(15, TimeUnit.SECONDS)).isTrue();
            // The app's connection drops while the message is being handled, before the ack.
            ((CachingConnectionFactory) ctx.getBean(ConnectionFactory.class)).resetConnection();
            release.countDown();
            work.next(); // the first try
            Delivery again = work.next();
            assertThat(again.message().getMessageProperties().isRedelivered()).isTrue();
            assertThat(again.message().getMessageProperties().getMessageId()).isEqualTo("m-5");
            assertThat(rabbit.receive("t5-orders.dlq", 500)).isNull();
        });
    }

    @Test
    void noThreadWaitsWhileAMessageWaits() throws Exception { // R-5
        TestWork work = new TestWork();
        work.behaviour = m -> "slow".equals(m.getMessageProperties().getMessageId())
                ? new IllegalStateException("down") : null;
        // One consumer thread, and a 2-second wait for the failing message.
        app("t6", work, "retry-ladder.initial-delay=2s", "retry-ladder.max-delay=2s",
                "retry-ladder.queues.t6-orders.window=1m").run(ctx -> {
            RabbitTemplate rabbit = ctx.getBean(RabbitTemplate.class);
            rabbit.send("", "t6-orders", message("slow", "{}"));
            Delivery slowFirst = work.next();
            rabbit.send("", "t6-orders", message("fast", "{}"));
            Delivery fast = work.next();
            assertThat(fast.message().getMessageProperties().getMessageId()).isEqualTo("fast");
            assertThat(fast.millisSince(slowFirst)).as("the other message didn't wait 2s").isLessThan(1_500);
            Delivery slowAgain = work.next();
            assertThat(slowAgain.message().getMessageProperties().getMessageId()).isEqualTo("slow");
            assertThat(slowAgain.millisSince(slowFirst)).isGreaterThanOrEqualTo(1_980);
        });
    }

    @Test
    void aRetryPlannedBeforeARestartArrivesAfterIt() throws Exception { // R-5
        TestWork before = new TestWork();
        String[] settings = {"retry-ladder.initial-delay=2s", "retry-ladder.max-delay=2s",
                "retry-ladder.queues.t7-orders.window=1m"};
        app("t7", before, settings).run(ctx -> {
            ctx.getBean(RabbitTemplate.class).send("", "t7-orders", message("m-7", "{}"));
            before.next();
            // Wait until the retry copy is in the delay queue, then shut the app down.
            RabbitAdmin admin = (RabbitAdmin) ctx.getBean(AmqpAdmin.class);
            long until = System.currentTimeMillis() + 5_000;
            while (admin.getQueueInfo("t7.delay.2000").getMessageCount() == 0 && System.currentTimeMillis() < until) {
                Thread.onSpinWait();
            }
            assertThat(admin.getQueueInfo("t7.delay.2000").getMessageCount()).isEqualTo(1);
        });
        TestWork after = new TestWork();
        after.behaviour = m -> null;
        app("t7", after, settings).run(ctx -> {
            Delivery d = after.next();
            assertThat(d.message().getMessageProperties().getMessageId()).isEqualTo("m-7");
            assertThat((Object) d.message().getMessageProperties().getHeader(Headers.RETRY_COUNT)).isEqualTo(1);
        });
    }

    @Test
    void twoQueuesWithDifferentTimeLimitsShareTheDelayQueues() throws Exception { // R-5
        TestWork work = new TestWork();
        app("t8", work, "retry-ladder.queues.t8-short.window=1s", "retry-ladder.queues.t8-long.window=1m").run(ctx -> {
            RabbitTemplate rabbit = ctx.getBean(RabbitTemplate.class);
            rabbit.send("", "t8-short", message("short", "{}"));
            rabbit.send("", "t8-long", message("long", "{}"));
            Message parked = rabbit.receive("t8-short.dlq", 15_000);
            assertThat(parked).isNotNull();
            assertThat((Object) parked.getMessageProperties().getHeader(Headers.EXHAUSTED_REASON)).isEqualTo("window");
            // The long one keeps going past the point where the short one stopped.
            long longTries = 0;
            long until = System.currentTimeMillis() + 15_000;
            while (longTries < 5 && System.currentTimeMillis() < until) {
                Delivery d = work.next();
                if ("long".equals(d.message().getMessageProperties().getMessageId())) {
                    longTries++;
                }
            }
            assertThat(longTries).isEqualTo(5);
            // Each retry came back only to its own queue, never to the other one (decision 1).
            for (Delivery d : work.seen) {
                MessageProperties p = d.message().getMessageProperties();
                assertThat(p.getConsumerQueue()).as(p.getMessageId()).isEqualTo("t8-" + p.getMessageId());
            }
            RabbitAdmin admin = (RabbitAdmin) ctx.getBean(AmqpAdmin.class);
            for (String q : List.of("t8.delay.200", "t8.delay.400", "t8.delay.800")) {
                assertThat(admin.getQueueInfo(q)).as(q).isNotNull();
            }
            assertThat(rabbit.receive("t8-long.dlq", 200)).isNull();
        });
    }

    @Test
    void eachRetryIsCountedAndLoggedWithoutTheBody(CapturedOutput output) throws Exception { // R-7
        TestWork work = new TestWork();
        // The test runner skips Spring Boot's logging setup, so turn on DEBUG for RetryLadder directly.
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(RetryLadder.class);
        ch.qos.logback.classic.Level before = logger.getLevel();
        logger.setLevel(ch.qos.logback.classic.Level.DEBUG);
        try {
            runLoggedRetries(work, output);
        } finally {
            logger.setLevel(before);
        }
    }

    private void runLoggedRetries(TestWork work, CapturedOutput output) {
        app("t9", work, "retry-ladder.queues.t9-orders.window=1s").run(ctx -> {
            RabbitTemplate rabbit = ctx.getBean(RabbitTemplate.class);
            rabbit.send("", "t9-orders", message("m-9", "{\"card\":\"SECRET-BODY-9\"}"));
            assertThat(rabbit.receive("t9-orders.dlq", 15_000)).isNotNull();
            MeterRegistry meters = ctx.getBean(MeterRegistry.class);
            assertThat(meters.counter("retry.ladder.scheduled", "queue", "t9-orders", "tier", "200").count()).isEqualTo(1);
            assertThat(meters.counter("retry.ladder.scheduled", "queue", "t9-orders", "tier", "400").count()).isEqualTo(1);
            assertThat(meters.counter("retry.ladder.exhausted", "queue", "t9-orders", "reason", "window").count())
                    .isEqualTo(1);
            assertThat(output).contains("Retry scheduled: messageId=m-9 queue=t9-orders retry=1 wait=200ms elapsed=0ms");
            assertThat(output).containsPattern("retry=2 wait=400ms elapsed=\\d+ms nextTryAt=\\S+ timeLeft=\\d+ms");
            assertThat(output).contains("Message m-9 parked in t9-orders.dlq: queue=t9-orders retryCount=2 reason=window");
            assertThat(output).doesNotContain("SECRET-BODY-9");
        });
    }

    @Test
    void workThatClearsTheHeadersCanNotRetryForever() throws Exception { // R-2
        TestWork work = new TestWork();
        work.behaviour = m -> {
            m.getMessageProperties().getHeaders().clear(); // a bug in the app's own code
            return new IllegalStateException("down");
        };
        app("t10", work, "retry-ladder.queues.t10-orders.max-attempts=2").run(ctx -> {
            RabbitTemplate rabbit = ctx.getBean(RabbitTemplate.class);
            rabbit.send("", "t10-orders", message("m-10", "{}"));
            Message parked = rabbit.receive("t10-orders.dlq", 15_000);
            assertThat(parked).as("parked after max-attempts, not retried forever").isNotNull();
            assertThat((Object) parked.getMessageProperties().getHeader(Headers.EXHAUSTED_REASON)).isEqualTo("attempts");
            assertThat(work.deliveries).hasSize(3);
        });
    }

    @Test
    void workThatChangesTheHeadersDoesNotChangeTheRetryCopy() throws Exception { // R-4
        TestWork work = new TestWork();
        work.behaviour = m -> {
            if (m.getMessageProperties().getHeader(Headers.RETRY_COUNT) != null) {
                return null; // the retry works
            }
            m.getMessageProperties().getHeaders().remove("app-header");
            return new IllegalStateException("down");
        };
        app("t11", work, "retry-ladder.queues.t11-orders.window=1m").run(ctx -> {
            ctx.getBean(RabbitTemplate.class).send("", "t11-orders", message("m-11", "{}"));
            work.next();
            Delivery retry = work.next();
            assertThat((Object) retry.message().getMessageProperties().getHeader("app-header")).isEqualTo("kept");
        });
    }

    @Test
    void workThatChangesThePropertiesDoesNotChangeTheRetryCopy() throws Exception { // R-4
        TestWork work = new TestWork();
        work.behaviour = m -> {
            if (m.getMessageProperties().getHeader(Headers.RETRY_COUNT) != null) {
                return null;
            }
            m.getMessageProperties().setMessageId("changed-by-the-work");
            return new IllegalStateException("down");
        };
        app("t12", work, "retry-ladder.queues.t12-orders.window=1m").run(ctx -> {
            ctx.getBean(RabbitTemplate.class).send("", "t12-orders", message("m-12", "{}"));
            work.next();
            assertThat(work.next().message().getMessageProperties().getMessageId()).isEqualTo("m-12");
        });
    }

    @Test
    void aPersistentMessageStaysPersistentOnAClassicQueue() throws Exception { // R-4
        TestWork work = new TestWork();
        work.behaviour = m -> m.getMessageProperties().getHeader(Headers.RETRY_COUNT) == null
                ? new IllegalStateException("down") : null;
        app("t13", work, "retry-ladder.queue-type=CLASSIC", "retry-ladder.queues.t13-orders.window=1m").run(ctx -> {
            Message sent = message("m-13", "{}");
            assertThat(sent.getMessageProperties().getDeliveryMode()).isEqualTo(MessageDeliveryMode.PERSISTENT);
            ctx.getBean(RabbitTemplate.class).send("", "t13-orders", sent);
            work.next();
            assertThat(work.next().message().getMessageProperties().getReceivedDeliveryMode())
                    .as("delivery mode of the retry").isEqualTo(MessageDeliveryMode.PERSISTENT);
        });
    }

    @Test
    void anErrorFromTheWorkParksTheMessageAndTheListenerKeepsGoing() throws Exception { // R-4
        TestWork work = new TestWork();
        work.behaviour = m -> {
            if ("boom".equals(m.getMessageProperties().getMessageId())) {
                throw new AssertionError("a bug in the app's code");
            }
            return null;
        };
        app("t14", work, "retry-ladder.queues.t14-orders.window=1m").run(ctx -> {
            RabbitTemplate rabbit = ctx.getBean(RabbitTemplate.class);
            Message sent = message("boom", "{}");
            rabbit.send("", "t14-orders", sent);
            Message parked = rabbit.receive("t14-orders.dlq", 15_000);
            assertThat(parked).as("parked by the queue's dead-letter setting").isNotNull();
            assertUnchanged(parked, sent);
            MeterRegistry meters = ctx.getBean(MeterRegistry.class);
            assertThat(meters.counter("retry.ladder.work.errors", "queue", "t14-orders").count()).isEqualTo(1);
            assertThat(meters.find("retry.ladder.park.failed").counter()).as("nothing failed to park").isNull();
            rabbit.send("", "t14-orders", message("next", "{}"));
            work.next(); // boom
            assertThat(work.next().message().getMessageProperties().getMessageId()).isEqualTo("next");
        });
    }

    @Test
    void aReplayedMessageStartsAFreshHistory() throws Exception { // R-2, R-6, decision 10
        TestWork work = new TestWork();
        java.util.concurrent.atomic.AtomicInteger tries = new java.util.concurrent.atomic.AtomicInteger();
        work.behaviour = m -> tries.getAndIncrement() == 0 ? new IllegalStateException("down") : null;
        app("t15", work, "retry-ladder.queues.t15-orders.window=1m").run(ctx -> {
            // Parked yesterday after its time ran out, then moved back from t15-orders.dlq after a fix.
            Message replayed = message("m-15", "{}");
            long yesterday = System.currentTimeMillis() - 86_400_000L;
            MessageProperties p = replayed.getMessageProperties();
            p.setHeader(Headers.RETRY_COUNT, 5);
            p.setHeader(Headers.FIRST_FAILED_AT, yesterday);
            p.setHeader(Headers.LAST_RETRY_AT, yesterday);
            p.setHeader(Headers.RETRY_DELAY_MS, 800L);
            p.setHeader(Headers.EXHAUSTED_REASON, "window");
            p.setHeader(Headers.EXHAUSTED_AT, yesterday);
            long sentAt = System.currentTimeMillis();
            ctx.getBean(RabbitTemplate.class).send("", "t15-orders", replayed);
            work.next();
            MessageProperties retry = work.next().message().getMessageProperties();
            assertThat((Object) retry.getHeader(Headers.RETRY_COUNT)).isEqualTo(1);
            assertThat((Long) retry.getHeader(Headers.FIRST_FAILED_AT)).isGreaterThanOrEqualTo(sentAt);
            assertThat(retry.getHeaders()).doesNotContainKeys(Headers.EXHAUSTED_REASON, Headers.EXHAUSTED_AT);
        });
    }
}

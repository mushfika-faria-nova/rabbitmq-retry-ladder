package io.github.mushfikafarianova.retryladder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;

import io.github.mushfikafarianova.retryladder.RetryLadderProperties.QueueType;

/** Names and declarations of everything the library creates in RabbitMQ (02-design.md section 3). */
final class Topology {

    /** Generous size limit for each delay queue; dead-lettered messages count against it (decision 2). */
    static final int DELAY_QUEUE_MAX_LENGTH = 1_000_000;

    private final String prefix;

    Topology(String prefix) {
        this.prefix = prefix;
    }

    String retryExchange() { return prefix + ".retry"; }
    String returnExchange() { return prefix + ".return"; }
    String parkingExchange() { return prefix + ".dlx"; }
    String delayQueue(Duration wait) { return prefix + ".delay." + wait.toMillis(); }
    String delayRoutingKey(Duration wait) { return "delay." + wait.toMillis(); }
    static String parkingQueue(String queue) { return queue + ".dlq"; }

    List<Declarable> declarables(List<QueuePlan> plans, QueueType appQueueType) {
        List<Declarable> d = new ArrayList<>();
        DirectExchange retry = new DirectExchange(retryExchange());
        HeadersExchange back = new HeadersExchange(returnExchange());
        DirectExchange parking = new DirectExchange(parkingExchange());
        d.add(retry);
        d.add(back);
        d.add(parking);

        // One delay queue per different wait, shared by every app queue (decision 4, R-5).
        TreeSet<Duration> waits = new TreeSet<>();
        plans.forEach(p -> waits.addAll(p.waits()));
        for (Duration wait : waits) {
            Queue delay = QueueBuilder.durable(delayQueue(wait)).quorum()
                    .ttl((int) wait.toMillis())
                    .deadLetterExchange(returnExchange())
                    .withArgument("x-dead-letter-strategy", "at-least-once")
                    .withArgument("x-overflow", "reject-publish")
                    .maxLength(DELAY_QUEUE_MAX_LENGTH)
                    .build();
            d.add(delay);
            d.add(new Binding(delay.getName(), Binding.DestinationType.QUEUE, retry.getName(),
                    delayRoutingKey(wait), null));
        }

        for (QueuePlan plan : plans) {
            String name = plan.queue();
            QueueBuilder app = QueueBuilder.durable(name)
                    .deadLetterExchange(parkingExchange())
                    .deadLetterRoutingKey(parkingQueue(name));
            if (appQueueType == QueueType.QUORUM) {
                app.quorum();
            }
            d.add(app.build());
            Queue dlq = QueueBuilder.durable(parkingQueue(name)).quorum().build();
            d.add(dlq);
            d.add(new Binding(dlq.getName(), Binding.DestinationType.QUEUE, parking.getName(), dlq.getName(), null));
            // all-with-x: plain "all" ignores x- headers and would take every message (decision 1).
            d.add(new Binding(name, Binding.DestinationType.QUEUE, back.getName(), "",
                    Map.of("x-match", "all-with-x", Headers.ORIGINAL_QUEUE, name)));
        }
        return d;
    }
}

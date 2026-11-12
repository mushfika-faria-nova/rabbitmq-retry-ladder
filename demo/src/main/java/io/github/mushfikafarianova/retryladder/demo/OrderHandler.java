package io.github.mushfikafarianova.retryladder.demo;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.Message;
import org.springframework.stereotype.Component;

/**
 * Handles orders, and is safe to run twice: the library delivers at least once, so the same order can
 * arrive again. It remembers the message IDs it has finished, and skips a repeat (R-4).
 * A real app would keep these IDs in its database, in the same transaction as the work.
 */
@Component
public class OrderHandler {

    private static final Log log = LogFactory.getLog(OrderHandler.class);

    private final FlakyDownstream downstream;
    private final Set<String> done = ConcurrentHashMap.newKeySet();
    private final AtomicInteger processed = new AtomicInteger();
    private final AtomicInteger skipped = new AtomicInteger();

    public OrderHandler(FlakyDownstream downstream) {
        this.downstream = downstream;
    }

    public void handle(Message message) {
        String id = message.getMessageProperties().getMessageId();
        if (done.contains(id)) {
            skipped.incrementAndGet();
            log.info("Order " + id + " was already processed, skipping the repeat");
            return;
        }
        downstream.call("order " + id);
        done.add(id);
        processed.incrementAndGet();
        log.info("Order " + id + " processed: " + new String(message.getBody(), StandardCharsets.UTF_8));
    }

    public int processed() {
        return processed.get();
    }

    public int skipped() {
        return skipped.get();
    }
}

package io.github.mushfikafarianova.retryladder;

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.github.mushfikafarianova.retryladder.Decision.Park;
import io.github.mushfikafarianova.retryladder.Decision.Reason;
import io.github.mushfikafarianova.retryladder.Decision.Retry;

/**
 * Decides, for one failed message, whether to retry or park it (R-2, R-3), and which history headers
 * to set (R-6). Plain logic: no RabbitMQ. The order of the checks is in 02-design.md section 2.
 */
final class StopRule {

    private static final Log log = LogFactory.getLog(StopRule.class);

    private final QueuePlan plan;
    private final List<Class<? extends Throwable>> nonRetryable;
    private final Clock clock;

    StopRule(QueuePlan plan, List<Class<? extends Throwable>> nonRetryable, Clock clock) {
        this.plan = plan;
        this.nonRetryable = nonRetryable;
        this.clock = clock;
    }

    /**
     * Only a parked message has x-retry-exhausted-reason, so a message with it was moved back after a fix.
     * Drops its old history so it starts again (decision 10). Returns true if it did.
     */
    static boolean resetIfReplayed(Map<String, Object> headers) {
        if (!headers.containsKey(Headers.EXHAUSTED_REASON)) {
            return false;
        }
        Headers.HISTORY.forEach(headers::remove);
        return true;
    }

    Decision decide(Map<String, Object> headers, Throwable error) {
        long now = clock.millis();
        int retries = retryCount(headers);
        Long firstFailedAt = millis(headers.get(Headers.FIRST_FAILED_AT));
        // No usable start time and no retries yet: a broken value counts as missing here (rule 3).
        boolean firstFailure = firstFailedAt == null && retries == 0;
        if (firstFailure) {
            firstFailedAt = now;
        }

        // 1. An error that can never succeed.
        if (isNonRetryable(error)) {
            return park(Reason.NON_RETRYABLE, retries, firstFailedAt, now);
        }
        // 2. Too many tries.
        if (retries >= plan.maxAttempts()) {
            return park(Reason.ATTEMPTS, retries, firstFailedAt, now);
        }
        Duration wait = plan.waitFor(retries);
        // 3 and 4. The time limit, counted from the first failure.
        if (firstFailedAt != null) {
            long elapsed = now - firstFailedAt;
            if (elapsed + wait.toMillis() > plan.window().toMillis()) {
                return park(Reason.WINDOW, retries, firstFailedAt, now);
            }
        } else {
            // 5. The start time is missing or broken: rely on the try limit. Never stamp a new one,
            // or the clock would restart on every retry.
            log.warn("Message on queue '" + plan.queue() + "' has no usable " + Headers.FIRST_FAILED_AT
                    + "; only max-attempts can stop its retries");
        }
        return retry(wait, retries, firstFailedAt, now);
    }

    private Retry retry(Duration wait, int retries, Long firstFailedAt, long now) {
        Map<String, Object> h = history(retries + 1, firstFailedAt);
        h.put(Headers.LAST_RETRY_AT, now);
        h.put(Headers.RETRY_DELAY_MS, wait.toMillis());
        return new Retry(wait, h);
    }

    private Park park(Reason reason, int retries, Long firstFailedAt, long now) {
        Map<String, Object> h = history(retries, firstFailedAt);
        h.put(Headers.EXHAUSTED_REASON, reason.value());
        h.put(Headers.EXHAUSTED_AT, now);
        return new Park(reason, h);
    }

    private Map<String, Object> history(int retryCount, Long firstFailedAt) {
        Map<String, Object> h = new LinkedHashMap<>();
        h.put(Headers.RETRY_COUNT, retryCount);
        if (firstFailedAt != null) {
            h.put(Headers.FIRST_FAILED_AT, firstFailedAt);
        }
        h.put(Headers.ORIGINAL_QUEUE, plan.queue());
        return h;
    }

    /** True if the error, or any error inside it, is on the list (or a subclass of one). */
    private boolean isNonRetryable(Throwable error) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable t = error; t != null && seen.add(t); t = t.getCause()) {
            for (Class<? extends Throwable> type : nonRetryable) {
                if (type.isInstance(t)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** A missing or broken count counts as no retries yet; the time limit still applies. */
    private static int retryCount(Map<String, Object> headers) {
        Long n = millis(headers.get(Headers.RETRY_COUNT));
        return n == null || n < 0 ? 0 : (int) Math.min(n, Integer.MAX_VALUE);
    }

    /** Reads a header written as a number, or as digits in a string. Anything else is broken. */
    private static Long millis(Object value) {
        if (value instanceof Number n) {
            return n.longValue();
        }
        if (value instanceof String s) {
            try {
                return Long.parseLong(s.trim());
            } catch (NumberFormatException ex) {
                return null;
            }
        }
        return null;
    }
}

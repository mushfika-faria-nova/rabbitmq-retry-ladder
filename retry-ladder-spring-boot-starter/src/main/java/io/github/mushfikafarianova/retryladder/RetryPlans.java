package io.github.mushfikafarianova.retryladder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.util.ClassUtils;

import io.github.mushfikafarianova.retryladder.RetryLadderProperties.WaitSettings;

/** Checks the settings and works out each queue's waits. Bad settings stop startup (R-8). */
final class RetryPlans {

    /** Guards against settings that would need a huge number of delay queues. */
    static final int MAX_DELAY_QUEUES = 32;

    private final List<QueuePlan> plans;
    private final List<Class<? extends Throwable>> nonRetryable;

    private RetryPlans(List<QueuePlan> plans, List<Class<? extends Throwable>> nonRetryable) {
        this.plans = plans;
        this.nonRetryable = nonRetryable;
    }

    List<QueuePlan> plans() {
        return plans;
    }

    List<Class<? extends Throwable>> nonRetryable() {
        return nonRetryable;
    }

    static RetryPlans from(RetryLadderProperties props, ClassLoader classLoader) {
        require(props.getExchangePrefix() != null && !props.getExchangePrefix().isBlank(),
                "retry-ladder.exchange-prefix must not be empty");
        requirePositive(props.getConfirmTimeout(), "retry-ladder.confirm-timeout");
        List<Class<? extends Throwable>> nonRetryable = new ArrayList<>();
        for (String name : props.getNonRetryable()) {
            nonRetryable.add(throwable(name, classLoader));
        }
        WaitSettings defaults = new WaitSettings();
        defaults.setInitialDelay(props.getInitialDelay());
        defaults.setMultiplier(props.getMultiplier());
        defaults.setMaxDelay(props.getMaxDelay());
        defaults.setWindow(props.getWindow());
        defaults.setMaxAttempts(props.getMaxAttempts());
        List<QueuePlan> plans = new ArrayList<>();
        for (Map.Entry<String, WaitSettings> e : props.getQueues().entrySet()) {
            plans.add(plan(e.getKey(), defaults, e.getValue()));
        }
        return new RetryPlans(List.copyOf(plans), List.copyOf(nonRetryable));
    }

    private static QueuePlan plan(String queue, WaitSettings defaults, WaitSettings own) {
        String where = "retry-ladder.queues." + queue + ": ";
        Duration initial = pick(own.getInitialDelay(), defaults.getInitialDelay(), where + "initial-delay");
        Double multiplier = pick(own.getMultiplier(), defaults.getMultiplier(), where + "multiplier");
        Duration max = pick(own.getMaxDelay(), defaults.getMaxDelay(), where + "max-delay");
        Duration window = pick(own.getWindow(), defaults.getWindow(), where + "window");
        Integer maxAttempts = pick(own.getMaxAttempts(), defaults.getMaxAttempts(), where + "max-attempts");

        require(initial.toMillis() >= 1, where + "initial-delay must be at least 1ms");
        require(multiplier >= 1, where + "multiplier must be 1 or more");
        require(max.compareTo(initial) >= 0, where + "max-delay must not be less than initial-delay");
        // A delay queue's TTL is an int of milliseconds; a longer wait would overflow it.
        require(max.compareTo(Duration.ofDays(24)) < 0, where + "max-delay must be under 24 days");
        requirePositive(window, where + "window");
        // A window shorter than the first wait would park every failure at once (decision 12).
        require(window.compareTo(initial) >= 0, where + "window must not be less than initial-delay");
        require(maxAttempts >= 1, where + "max-attempts must be 1 or more");

        return new QueuePlan(queue, waits(initial, multiplier, max, where), window, maxAttempts);
    }

    /** First wait, then times the multiplier, until it reaches the longest wait. */
    private static List<Duration> waits(Duration initial, double multiplier, Duration max, String where) {
        if (multiplier == 1) {
            return List.of(initial);
        }
        List<Duration> waits = new ArrayList<>();
        long maxMs = max.toMillis();
        double next = initial.toMillis();
        while (Math.round(next) < maxMs) {
            long ms = Math.round(next);
            // Rounding to whole ms can repeat a value when the multiplier is close to 1.
            if (waits.isEmpty() || ms > waits.getLast().toMillis()) {
                waits.add(Duration.ofMillis(ms));
            }
            tooMany(waits.size(), where);
            // At least 1 ms more each step, so a multiplier just above 1 fails fast instead of looping for ever.
            next = Math.max(next * multiplier, ms + 1);
        }
        waits.add(max);
        tooMany(waits.size(), where);
        return List.copyOf(waits);
    }

    private static void tooMany(int count, String where) {
        require(count <= MAX_DELAY_QUEUES, where + "these settings need more than " + MAX_DELAY_QUEUES
                + " delay queues. Raise initial-delay or multiplier, or lower max-delay.");
    }

    private static Class<? extends Throwable> throwable(String name, ClassLoader classLoader) {
        Class<?> type;
        try {
            type = ClassUtils.forName(name, classLoader);
        } catch (ClassNotFoundException | LinkageError ex) {
            // The message names the class; the JDK's own exception adds nothing for the user.
            throw new IllegalStateException("retry-ladder.non-retryable: class not found: " + name);
        }
        require(Throwable.class.isAssignableFrom(type),
                "retry-ladder.non-retryable: " + name + " is not an exception");
        return type.asSubclass(Throwable.class);
    }

    private static <T> T pick(T own, T fallback, String name) {
        T value = own != null ? own : fallback;
        require(value != null, name + " is required");
        return value;
    }

    private static void requirePositive(Duration d, String name) {
        require(d != null && d.isPositive(), name + " must be more than 0");
    }

    private static void require(boolean ok, String message) {
        if (!ok) {
            throw new IllegalStateException(message);
        }
    }
}

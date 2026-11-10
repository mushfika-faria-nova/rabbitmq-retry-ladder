package io.github.mushfikafarianova.retryladder;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

/** The checked settings for one queue, and the waits they produce. */
record QueuePlan(String queue, List<Duration> waits, Duration window, int maxAttempts) {

    /** The line logged at startup, so a team can check its settings at a glance (R-8). */
    String describe() {
        String steps = waits.stream().map(QueuePlan::format).collect(Collectors.joining(", "));
        String last = format(waits.getLast());
        return "Retry plan for queue '%s': waits %s (then %s repeats), window %s, max attempts %d"
                .formatted(queue, steps, last, format(window), maxAttempts);
    }

    static String format(Duration d) {
        long ms = d.toMillis();
        if (ms % 3_600_000 == 0) return ms / 3_600_000 + "h";
        if (ms % 60_000 == 0) return ms / 60_000 + "m";
        if (ms % 1_000 == 0) return ms / 1_000 + "s";
        return ms + "ms";
    }
}

package io.github.mushfikafarianova.retryladder;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

/** R-1: the waits grow, then level off, and the number of delay queues comes from the settings only. */
class LadderTest {

    private static QueuePlan plan(String... settings) {
        RetryLadderProperties props = new RetryLadderProperties();
        props.getQueues().put("orders", new RetryLadderProperties.WaitSettings());
        for (String s : settings) {
            String[] kv = s.split("=");
            switch (kv[0]) {
                case "initial-delay" -> props.setInitialDelay(Duration.parse(kv[1]));
                case "multiplier" -> props.setMultiplier(Double.parseDouble(kv[1]));
                case "max-delay" -> props.setMaxDelay(Duration.parse(kv[1]));
                default -> throw new IllegalArgumentException(kv[0]);
            }
        }
        return RetryPlans.from(props, LadderTest.class.getClassLoader()).plans().getFirst();
    }

    @Test
    void theWaitsGrowThenLevelOff() {
        QueuePlan p = plan(); // defaults: 1m doubling to 8m
        assertThat(p.waits()).containsExactly(Duration.ofMinutes(1), Duration.ofMinutes(2),
                Duration.ofMinutes(4), Duration.ofMinutes(8));
        assertThat(p.waitFor(0)).isEqualTo(Duration.ofMinutes(1));
        assertThat(p.waitFor(1)).isEqualTo(Duration.ofMinutes(2));
        assertThat(p.waitFor(2)).isEqualTo(Duration.ofMinutes(4));
        assertThat(p.waitFor(3)).isEqualTo(Duration.ofMinutes(8));
        assertThat(p.waitFor(4)).isEqualTo(Duration.ofMinutes(8));
    }

    @Test
    void manyRetriesNeverAddDelayQueues() {
        QueuePlan p = plan();
        assertThat(p.waitFor(1_000)).isEqualTo(Duration.ofMinutes(8));
        assertThat(p.waits()).hasSize(4);
    }

    @Test
    void aFixedWaitUsesOneQueue() {
        QueuePlan p = plan("multiplier=1");
        assertThat(p.waits()).containsExactly(Duration.ofMinutes(1));
        assertThat(p.waitFor(5)).isEqualTo(Duration.ofMinutes(1));
    }

    @Test
    void aFirstWaitEqualToTheLongestUsesOneQueue() {
        assertThat(plan("max-delay=PT1M").waits()).containsExactly(Duration.ofMinutes(1));
    }

    @Test
    void aWaitThatDoesNotDivideEvenlyStillEndsAtTheLongestWait() {
        // 1m x 3 = 1m, 3m, 9m; 9m is past 8m, so the last wait is 8m.
        assertThat(plan("multiplier=3").waits()).containsExactly(Duration.ofMinutes(1), Duration.ofMinutes(3),
                Duration.ofMinutes(8));
    }
}

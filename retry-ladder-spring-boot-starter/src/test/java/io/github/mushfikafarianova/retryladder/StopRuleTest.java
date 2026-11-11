package io.github.mushfikafarianova.retryladder;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.github.mushfikafarianova.retryladder.Decision.Park;
import io.github.mushfikafarianova.retryladder.Decision.Reason;
import io.github.mushfikafarianova.retryladder.Decision.Retry;

/** R-2, R-3 and R-6: when to stop, and the history each message carries. Uses the defaults. */
class StopRuleTest {

    static class BadPayload extends RuntimeException {
    }

    static class VeryBadPayload extends BadPayload {
    }

    static final long T0 = 1_700_000_000_000L;
    static final long MIN = 60_000;

    // Waits 1m, 2m, 4m, 8m, then 8m; window 30m; max attempts 100.
    static final QueuePlan PLAN = new QueuePlan("orders",
            List.of(Duration.ofMinutes(1), Duration.ofMinutes(2), Duration.ofMinutes(4), Duration.ofMinutes(8)),
            Duration.ofMinutes(30), 100);

    static StopRule ruleAt(long now) {
        return new StopRule(PLAN, List.of(BadPayload.class), Clock.fixed(Instant.ofEpochMilli(now), ZoneOffset.UTC));
    }

    static Map<String, Object> headers(Object... kv) {
        Map<String, Object> h = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            h.put((String) kv[i], kv[i + 1]);
        }
        return h;
    }

    static final RuntimeException DOWN = new IllegalStateException("downstream is down");

    @Nested
    class TimeLimit { // R-2

        @Test
        void theFirstFailureStampsTheStartTimeAndRetriesAfterTheFirstWait() {
            Decision d = ruleAt(T0).decide(headers(), DOWN);
            assertThat(d).isInstanceOf(Retry.class);
            assertThat(((Retry) d).delay()).isEqualTo(Duration.ofMinutes(1));
            assertThat(d.headers()).containsEntry(Headers.FIRST_FAILED_AT, T0).containsEntry(Headers.RETRY_COUNT, 1);
        }

        @Test
        void aRetryLandingExactlyOnTheLimitIsAllowed() {
            // Failure 5 at 22m: the next wait is 8m, landing at exactly 30m.
            Decision d = ruleAt(T0 + 22 * MIN).decide(headers(Headers.RETRY_COUNT, 4, Headers.FIRST_FAILED_AT, T0), DOWN);
            assertThat(d).isInstanceOf(Retry.class);
            assertThat(((Retry) d).delay()).isEqualTo(Duration.ofMinutes(8));
        }

        @Test
        void oneMillisecondOverTheLimitIsParked() {
            Decision d = ruleAt(T0 + 22 * MIN + 1).decide(headers(Headers.RETRY_COUNT, 4, Headers.FIRST_FAILED_AT, T0), DOWN);
            assertThat(d).isInstanceOf(Park.class);
            assertThat(((Park) d).reason()).isEqualTo(Reason.WINDOW);
        }

        @Test
        void theDesignExampleIsParkedAtTheSixthFailure() {
            // 02-design.md section 1: failure 6 at 23m, an 8m wait would end at 31m.
            Decision d = ruleAt(T0 + 23 * MIN).decide(headers(Headers.RETRY_COUNT, 5, Headers.FIRST_FAILED_AT, T0), DOWN);
            assertThat(((Park) d).reason()).isEqualTo(Reason.WINDOW);
        }

        @Test
        void aStartTimeWrittenAsDigitsIsRead() {
            Decision d = ruleAt(T0 + 23 * MIN).decide(
                    headers(Headers.RETRY_COUNT, 5, Headers.FIRST_FAILED_AT, Long.toString(T0)), DOWN);
            assertThat(((Park) d).reason()).isEqualTo(Reason.WINDOW);
        }
    }

    @Nested
    class MissingStartTime { // R-2

        @Test
        void aRetriedMessageWithoutAStartTimeKeepsRetryingAndIsNeverStamped() {
            // Hours past any window, but with no start time only max-attempts can stop it.
            Decision d = ruleAt(T0 + 600 * MIN).decide(headers(Headers.RETRY_COUNT, 3), DOWN);
            assertThat(d).isInstanceOf(Retry.class);
            assertThat(d.headers()).doesNotContainKey(Headers.FIRST_FAILED_AT);
        }

        @Test
        void aBrokenStartTimeIsTreatedAsMissing() {
            Decision d = ruleAt(T0).decide(headers(Headers.RETRY_COUNT, 3, Headers.FIRST_FAILED_AT, "yesterday"), DOWN);
            assertThat(d).isInstanceOf(Retry.class);
            assertThat(d.headers()).doesNotContainKey(Headers.FIRST_FAILED_AT);
        }

        @Test
        void theTryLimitStopsAMessageWithoutAStartTime() {
            Decision d = ruleAt(T0).decide(headers(Headers.RETRY_COUNT, 100), DOWN);
            assertThat(((Park) d).reason()).isEqualTo(Reason.ATTEMPTS);
        }

        @Test
        void oneBelowTheTryLimitStillRetries() {
            assertThat(ruleAt(T0).decide(headers(Headers.RETRY_COUNT, 99), DOWN)).isInstanceOf(Retry.class);
        }
    }

    @Nested
    class NeverSucceeds { // R-3

        @Test
        void aListedErrorIsParkedAtOnce() {
            Decision d = ruleAt(T0).decide(headers(), new BadPayload());
            assertThat(((Park) d).reason()).isEqualTo(Reason.NON_RETRYABLE);
        }

        @Test
        void aListedErrorWrappedInsideAnotherIsParked() {
            Decision d = ruleAt(T0).decide(headers(), new RuntimeException("listener failed", new BadPayload()));
            assertThat(((Park) d).reason()).isEqualTo(Reason.NON_RETRYABLE);
        }

        @Test
        void aSubclassOfAListedErrorIsParked() {
            Decision d = ruleAt(T0).decide(headers(), new VeryBadPayload());
            assertThat(((Park) d).reason()).isEqualTo(Reason.NON_RETRYABLE);
        }

        @Test
        void anUnlistedErrorIsRetried() {
            assertThat(ruleAt(T0).decide(headers(), DOWN)).isInstanceOf(Retry.class);
        }

        @Test
        void anErrorThatIsItsOwnCauseDoesNotLoopForever() {
            RuntimeException a = new RuntimeException("a");
            RuntimeException b = new RuntimeException("b", a);
            a.initCause(b);
            assertThat(ruleAt(T0).decide(headers(), a)).isInstanceOf(Retry.class);
        }
    }

    @Nested
    class History { // R-6

        @Test
        void aRetryCopyCarriesItsWholeHistory() {
            Decision d = ruleAt(T0 + 3 * MIN).decide(headers(Headers.RETRY_COUNT, 2, Headers.FIRST_FAILED_AT, T0), DOWN);
            assertThat(d.headers())
                    .containsEntry(Headers.RETRY_COUNT, 3)
                    .containsEntry(Headers.FIRST_FAILED_AT, T0)
                    .containsEntry(Headers.ORIGINAL_QUEUE, "orders")
                    .containsEntry(Headers.LAST_RETRY_AT, T0 + 3 * MIN)
                    .containsEntry(Headers.RETRY_DELAY_MS, 4 * MIN)
                    .doesNotContainKeys(Headers.EXHAUSTED_REASON, Headers.EXHAUSTED_AT);
        }

        @Test
        void aParkedMessageSaysWhenAndWhyItStopped() {
            Decision d = ruleAt(T0 + 23 * MIN).decide(headers(Headers.RETRY_COUNT, 5, Headers.FIRST_FAILED_AT, T0), DOWN);
            assertThat(d.headers())
                    .containsEntry(Headers.RETRY_COUNT, 5)
                    .containsEntry(Headers.FIRST_FAILED_AT, T0)
                    .containsEntry(Headers.ORIGINAL_QUEUE, "orders")
                    .containsEntry(Headers.EXHAUSTED_REASON, "window")
                    .containsEntry(Headers.EXHAUSTED_AT, T0 + 23 * MIN);
        }

        @Test
        void aMessageParkedOnItsFirstFailureStillGetsAStartTime() {
            Decision d = ruleAt(T0).decide(headers(), new BadPayload());
            assertThat(d.headers())
                    .containsEntry(Headers.RETRY_COUNT, 0)
                    .containsEntry(Headers.FIRST_FAILED_AT, T0)
                    .containsEntry(Headers.EXHAUSTED_REASON, "non-retryable");
        }
    }
}

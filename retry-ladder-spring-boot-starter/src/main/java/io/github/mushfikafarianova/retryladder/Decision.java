package io.github.mushfikafarianova.retryladder;

import java.time.Duration;
import java.util.Map;

/** What to do with a failed message: retry it after a wait, or park it. */
sealed interface Decision {

    /** Headers to set on the retry copy or the parked message. */
    Map<String, Object> headers();

    /** Send a copy to the delay queue for {@code delay}. */
    record Retry(Duration delay, Map<String, Object> headers) implements Decision {
    }

    /** Send the message to its parking queue. */
    record Park(Reason reason, Map<String, Object> headers) implements Decision {
    }

    /** Why a message stopped being retried. The value goes in {@code x-retry-exhausted-reason}. */
    enum Reason {
        WINDOW("window"),
        ATTEMPTS("attempts"),
        NON_RETRYABLE("non-retryable");

        private final String value;

        Reason(String value) {
            this.value = value;
        }

        String value() {
            return value;
        }
    }
}

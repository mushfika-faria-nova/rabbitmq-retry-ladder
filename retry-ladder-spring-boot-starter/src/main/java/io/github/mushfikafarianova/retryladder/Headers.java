package io.github.mushfikafarianova.retryladder;

/** Names of the headers the library adds to a message (R-6). */
final class Headers {

    static final String RETRY_COUNT = "x-retry-count";
    static final String FIRST_FAILED_AT = "x-first-failed-at";
    static final String ORIGINAL_QUEUE = "x-original-queue";
    static final String LAST_RETRY_AT = "x-last-retry-at";
    static final String RETRY_DELAY_MS = "x-retry-delay-ms";
    static final String EXHAUSTED_REASON = "x-retry-exhausted-reason";
    static final String EXHAUSTED_AT = "x-exhausted-at";

    private Headers() {
    }
}

package io.github.mushfikafarianova.retryladder.demo;

/**
 * An order without a messageId. The handler can't skip a repeat without one, so it isn't safe to run twice.
 * Listed under retry-ladder.non-retryable, so it's parked at once.
 */
public class MissingMessageIdException extends RuntimeException {

    public MissingMessageIdException(String message) {
        super(message);
    }
}

package io.github.mushfikafarianova.retryladder.demo;

/** A payment that can never succeed. Listed under retry-ladder.non-retryable, so it's parked at once. */
public class BadPaymentException extends RuntimeException {

    public BadPaymentException(String message) {
        super(message);
    }
}

package io.github.mushfikafarianova.retryladder.demo;

import java.time.Duration;
import java.time.Instant;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** A pretend downstream system that is down for a while after startup, then comes back. */
@Component
public class FlakyDownstream {

    private final Instant upAt;

    public FlakyDownstream(@Value("${demo.downstream-down-for}") Duration downFor) {
        this.upAt = Instant.now().plus(downFor);
    }

    public void call(String what) {
        if (Instant.now().isBefore(upAt)) {
            throw new IllegalStateException("downstream is down, can't handle " + what);
        }
    }
}

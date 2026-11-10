package io.github.mushfikafarianova.retryladder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Settings under {@code retry-ladder}. The top-level wait settings are the defaults;
 * each queue under {@code queues} can override any of them.
 */
@ConfigurationProperties("retry-ladder")
public class RetryLadderProperties {

    /** Name prefix for the exchanges and delay queues the library creates. */
    private String exchangePrefix = "retry-ladder";

    /** How long to wait for RabbitMQ to confirm a retry copy or a parked message. */
    private Duration confirmTimeout = Duration.ofSeconds(5);

    /** Exception classes that can never succeed. A message failing with one is parked at once. */
    private List<String> nonRetryable = new ArrayList<>();

    // The default wait settings for every queue: waits of 1m, 2m, 4m, 8m, then 8m repeating, for up to 30 minutes.

    /** First wait after a failure. */
    private Duration initialDelay = Duration.ofMinutes(1);

    /** Each wait is the previous one times this. 1 means a fixed wait. */
    private double multiplier = 2;

    /** The waits stop growing at this value. */
    private Duration maxDelay = Duration.ofMinutes(8);

    /** Stop retrying this long after the first failure. */
    private Duration window = Duration.ofMinutes(30);

    /** Safety limit on tries, used when the time limit can't be worked out. */
    private int maxAttempts = 100;

    /** The queues the library manages, by name. Each can override the default wait settings. */
    private Map<String, WaitSettings> queues = new LinkedHashMap<>();

    public String getExchangePrefix() { return exchangePrefix; }
    public void setExchangePrefix(String exchangePrefix) { this.exchangePrefix = exchangePrefix; }

    public Duration getConfirmTimeout() { return confirmTimeout; }
    public void setConfirmTimeout(Duration confirmTimeout) { this.confirmTimeout = confirmTimeout; }

    public List<String> getNonRetryable() { return nonRetryable; }
    public void setNonRetryable(List<String> nonRetryable) { this.nonRetryable = nonRetryable; }

    public Duration getInitialDelay() { return initialDelay; }
    public void setInitialDelay(Duration initialDelay) { this.initialDelay = initialDelay; }

    public double getMultiplier() { return multiplier; }
    public void setMultiplier(double multiplier) { this.multiplier = multiplier; }

    public Duration getMaxDelay() { return maxDelay; }
    public void setMaxDelay(Duration maxDelay) { this.maxDelay = maxDelay; }

    public Duration getWindow() { return window; }
    public void setWindow(Duration window) { this.window = window; }

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public Map<String, WaitSettings> getQueues() { return queues; }
    public void setQueues(Map<String, WaitSettings> queues) { this.queues = queues; }

    /** Wait settings for one queue. A value left empty uses the default. */
    public static class WaitSettings {

        /** First wait after a failure. */
        private Duration initialDelay;

        /** Each wait is the previous one times this. 1 means a fixed wait. */
        private Double multiplier;

        /** The waits stop growing at this value. */
        private Duration maxDelay;

        /** Stop retrying this long after the first failure. */
        private Duration window;

        /** Safety limit on tries, used when the time limit can't be worked out. */
        private Integer maxAttempts;

        public Duration getInitialDelay() { return initialDelay; }
        public void setInitialDelay(Duration initialDelay) { this.initialDelay = initialDelay; }

        public Double getMultiplier() { return multiplier; }
        public void setMultiplier(Double multiplier) { this.multiplier = multiplier; }

        public Duration getMaxDelay() { return maxDelay; }
        public void setMaxDelay(Duration maxDelay) { this.maxDelay = maxDelay; }

        public Duration getWindow() { return window; }
        public void setWindow(Duration window) { this.window = window; }

        public Integer getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(Integer maxAttempts) { this.maxAttempts = maxAttempts; }
    }
}

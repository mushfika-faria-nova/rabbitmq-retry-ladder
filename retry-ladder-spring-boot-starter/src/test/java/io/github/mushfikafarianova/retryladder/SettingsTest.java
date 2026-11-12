package io.github.mushfikafarianova.retryladder;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

/** R-8: bad settings stop startup, and the retry plan is logged at startup. */
@ExtendWith(OutputCaptureExtension.class)
class SettingsTest {

    // The README example: 1m doubling to 8m, a 30-minute window, payments overrides the window.
    private static final String[] GOOD = {
            "retry-ladder.initial-delay=1m",
            "retry-ladder.multiplier=2",
            "retry-ladder.max-delay=8m",
            "retry-ladder.window=30m",
            "retry-ladder.queues.orders.max-attempts=100",
            "retry-ladder.queues.payments.window=2h",
    };

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RabbitAutoConfiguration.class, RetryLadderAutoConfiguration.class))
            .withPropertyValues(GOOD);

    @Test
    void goodSettingsStartAndLogEachQueuesPlan(CapturedOutput output) {
        runner.run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(RetryPlans.class);
            assertThat(output).contains("Retry plan for queue 'orders': waits 1m, 2m, 4m, 8m (then 8m repeats), "
                    + "window 30m, max attempts 100");
            assertThat(output).contains("Retry plan for queue 'payments': waits 1m, 2m, 4m, 8m (then 8m repeats), "
                    + "window 2h, max attempts 100");
        });
    }

    @Test
    void aFixedWaitUsesOneDelayQueue(CapturedOutput output) {
        runner.withPropertyValues("retry-ladder.queues.orders.multiplier=1").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(output).contains("Retry plan for queue 'orders': waits 1m (then 1m repeats)");
        });
    }

    @Test
    void aQueueNameWithDotsGoesInBrackets(CapturedOutput output) {
        runner.withPropertyValues("retry-ladder.queues[notifications.delivery-status].window=1h").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(output).contains("Retry plan for queue 'notifications.delivery-status':");
        });
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource(delimiter = '|', value = {
            "retry-ladder.initial-delay=0s               | retry-ladder.queues.orders: initial-delay must be at least 1ms",
            "retry-ladder.multiplier=0.5                 | retry-ladder.queues.orders: multiplier must be 1 or more",
            "retry-ladder.max-delay=30s                  | retry-ladder.queues.orders: max-delay must not be less than initial-delay",
            "retry-ladder.max-delay=25d                  | retry-ladder.queues.orders: max-delay must be under 24 days",
            "retry-ladder.max-delay=24d                  | retry-ladder.queues.orders: max-delay must be under 24 days",
            "retry-ladder.window=0s                      | retry-ladder.queues.orders: window must be more than 0",
            "retry-ladder.window=59s                     | retry-ladder.queues.orders: window must not be less than initial-delay",
            "retry-ladder.queues.payments.max-attempts=0 | retry-ladder.queues.payments: max-attempts must be 1 or more",
            "retry-ladder.queues.payments.window=-1m     | retry-ladder.queues.payments: window must be more than 0",
            "retry-ladder.confirm-timeout=0s             | retry-ladder.confirm-timeout must be more than 0",
            "retry-ladder.exchange-prefix=               | retry-ladder.exchange-prefix must not be empty",
            "retry-ladder.non-retryable=com.example.Nope | retry-ladder.non-retryable: class not found: com.example.Nope",
            "retry-ladder.non-retryable=java.lang.String | retry-ladder.non-retryable: java.lang.String is not an exception",
    })
    void badSettingsStopStartup(String setting, String message) {
        runner.withPropertyValues(setting).run(context ->
                assertThat(context).getFailure().rootCause().hasMessage(message));
    }

    @Test
    void leftOutSettingsUseTheDefaults(CapturedOutput output) {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RabbitAutoConfiguration.class, RetryLadderAutoConfiguration.class))
                .withPropertyValues("retry-ladder.queues.orders.max-attempts=100")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(output).contains("Retry plan for queue 'orders': waits 1m, 2m, 4m, 8m "
                            + "(then 8m repeats), window 30m, max attempts 100");
                });
    }

    @Test
    void aMaxDelayJustUnder24DaysIsAllowed() {
        runner.withPropertyValues("retry-ladder.max-delay=PT575H59M59.999S")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void aWindowEqualToTheFirstWaitIsAllowed() {
        runner.withPropertyValues("retry-ladder.window=1m").run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    @Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD) // a busy loop ignores interrupts
    void aMultiplierJustAbove1FailsFastInsteadOfHanging() {
        runner.withPropertyValues("retry-ladder.multiplier=1.000000000001").run(context -> assertThat(context)
                .getFailure().rootCause()
                .hasMessage("retry-ladder.queues.orders: these settings need more than 32 delay queues. "
                        + "Raise initial-delay or multiplier, or lower max-delay."));
    }

    @Test
    void settingsThatNeedTooManyDelayQueuesStopStartup() {
        runner.withPropertyValues("retry-ladder.initial-delay=1ms", "retry-ladder.multiplier=1.01",
                "retry-ladder.max-delay=1h").run(context -> assertThat(context).getFailure().rootCause()
                .hasMessage("retry-ladder.queues.orders: these settings need more than 32 delay queues. "
                        + "Raise initial-delay or multiplier, or lower max-delay."));
    }
}

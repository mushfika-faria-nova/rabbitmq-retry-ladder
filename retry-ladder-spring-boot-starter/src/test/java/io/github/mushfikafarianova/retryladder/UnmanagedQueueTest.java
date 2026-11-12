package io.github.mushfikafarianova.retryladder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Clock;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import com.rabbitmq.client.Channel;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/** A queue that isn't under retry-ladder.queues is reported on its first message, not only on its first failure. */
@ExtendWith(OutputCaptureExtension.class)
class UnmanagedQueueTest {

    static Message from(String queue, long tag) {
        MessageProperties p = new MessageProperties();
        p.setConsumerQueue(queue);
        p.setDeliveryTag(tag);
        return new Message(new byte[0], p);
    }

    @Test
    void theFirstMessageFromAQueueThatIsNotListedLogsAnErrorOnce(CapturedOutput output) throws Exception {
        RetryLadderProperties props = new RetryLadderProperties();
        props.getQueues().put("orders", new RetryLadderProperties.WaitSettings());
        RetryLadder ladder = new RetryLadder(RetryPlans.from(props, getClass().getClassLoader()), new Topology("t"),
                mock(RabbitTemplate.class), Duration.ofSeconds(1), new SimpleMeterRegistry(), Clock.systemUTC());
        Channel channel = mock(Channel.class);

        ladder.handle(from("shipments", 1), channel, m -> { });
        ladder.handle(from("shipments", 2), channel, m -> { });
        ladder.handle(from("orders", 3), channel, m -> { });

        verify(channel).basicAck(1, false); // a message that works is still acked
        String warning = "Queue 'shipments' isn't under retry-ladder.queues";
        assertThat(output.getAll().split(java.util.regex.Pattern.quote(warning), -1)).as("logged once").hasSize(2);
        assertThat(output).doesNotContain("Queue 'orders' isn't under");
    }
}

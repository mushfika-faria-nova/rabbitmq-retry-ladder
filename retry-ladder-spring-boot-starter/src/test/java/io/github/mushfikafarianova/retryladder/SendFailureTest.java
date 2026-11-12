package io.github.mushfikafarianova.retryladder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.time.Clock;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import com.rabbitmq.client.Channel;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/** R-4: an unexpected failure while sending a copy never leaves the message stuck. RabbitMQ is mocked. */
class SendFailureTest {

    @Test
    void anUnexpectedExceptionWhileSendingRejectsTheMessage() throws Exception {
        RetryLadderProperties props = new RetryLadderProperties();
        props.getQueues().put("orders", new RetryLadderProperties.WaitSettings());
        RabbitTemplate template = mock(RabbitTemplate.class);
        doThrow(new IllegalStateException("a bug while sending")).when(template)
                .send(anyString(), anyString(), any(Message.class), any(CorrelationData.class));
        MeterRegistry meters = new SimpleMeterRegistry();
        RetryLadder ladder = new RetryLadder(RetryPlans.from(props, getClass().getClassLoader()), new Topology("t"),
                template, Duration.ofSeconds(1), meters, Clock.systemUTC());

        MessageProperties p = new MessageProperties();
        p.setConsumerQueue("orders");
        p.setDeliveryTag(7);
        p.setMessageId("m-1");
        Channel channel = mock(Channel.class);

        assertThatCode(() -> ladder.handle(new Message(new byte[0], p), channel, m -> {
            throw new IllegalStateException("downstream is down");
        })).doesNotThrowAnyException();
        verify(channel).basicReject(7, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        assertThat(meters.counter("retry.ladder.park.failed", "queue", "orders", "cause", "error").count())
                .isEqualTo(1);
    }
}

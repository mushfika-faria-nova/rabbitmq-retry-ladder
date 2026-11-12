package io.github.mushfikafarianova.retryladder.demo;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;

/** The demo app against a real RabbitMQ, with the waits in milliseconds. */
@Testcontainers
@SpringBootTest(properties = {
        "demo.send-on-startup=false",
        "demo.downstream-down-for=1s",
        "retry-ladder.initial-delay=200ms",
        "retry-ladder.max-delay=400ms",
        "retry-ladder.window=30s",
})
class DemoTest {

    @Container
    @ServiceConnection
    static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:4.1-management");

    @Autowired
    RabbitTemplate rabbit;

    @Autowired
    OrderHandler orders;

    static Message message(String id, String body) {
        MessageProperties p = new MessageProperties();
        p.setMessageId(id);
        p.setContentType("application/json");
        return new Message(body.getBytes(StandardCharsets.UTF_8), p);
    }

    @Test
    void anOrderThatFinallyWorksIsProcessedOnceEvenWhenItArrivesTwice() throws Exception { // R-4
        // Both copies fail while the downstream is down, both are retried, both finally get through.
        rabbit.send("", "orders", message("order-9", "{\"order\":9}"));
        rabbit.send("", "orders", message("order-9", "{\"order\":9}"));
        long until = System.currentTimeMillis() + 20_000;
        while (orders.processed() + orders.skipped() < 2 && System.currentTimeMillis() < until) {
            Thread.onSpinWait();
        }
        assertThat(orders.processed()).as("processed").isEqualTo(1);
        assertThat(orders.skipped()).as("skipped as a repeat").isEqualTo(1);
        assertThat(rabbit.receive("orders.dlq", 200)).isNull();
    }

    @Test
    void anOrderWithoutAnIdIsParkedAtOnce() {
        // Without an ID the handler can't skip a repeat, so it isn't retried.
        rabbit.send("", "orders", message(null, "{\"order\":10}"));
        Message parked = rabbit.receive("orders.dlq", 15_000);
        assertThat(parked).isNotNull();
        assertThat((Object) parked.getMessageProperties().getHeader("x-retry-exhausted-reason"))
                .isEqualTo("non-retryable");
    }

    @Test
    void aBadPaymentIsParkedAtOnce() {
        rabbit.send("", "payments", message("payment-9", "{\"payment\":9,\"amount\":-5}"));
        Message parked = rabbit.receive("payments.dlq", 15_000);
        assertThat(parked).isNotNull();
        assertThat((Object) parked.getMessageProperties().getHeader("x-retry-exhausted-reason"))
                .isEqualTo("non-retryable");
    }
}

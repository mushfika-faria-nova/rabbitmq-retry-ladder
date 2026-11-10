package io.github.mushfikafarianova.retryladder;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;

/**
 * Proves the RabbitMQ behaviour the design depends on (decisions 1 and 2), before any retry code exists:
 * a quorum delay queue with at-least-once dead-lettering sends an expired message back through a
 * headers exchange, with its body, properties and x- headers unchanged.
 */
@Testcontainers
class BrokerSmokeTest {

    @Container
    static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:4.1-management");

    static CachingConnectionFactory connectionFactory;
    static RabbitAdmin admin;
    static RabbitTemplate template;

    @BeforeAll
    static void connect() {
        connectionFactory = new CachingConnectionFactory(RABBIT.getHost(), RABBIT.getAmqpPort());
        connectionFactory.setUsername(RABBIT.getAdminUsername());
        connectionFactory.setPassword(RABBIT.getAdminPassword());
        connectionFactory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.SIMPLE);
        admin = new RabbitAdmin(connectionFactory);
        template = new RabbitTemplate(connectionFactory);
    }

    @AfterAll
    static void disconnect() {
        connectionFactory.destroy();
    }

    @Test
    void anExpiredMessageGoesBackToItsQueueUnchanged() {
        HeadersExchange returns = new HeadersExchange("smoke.return");
        Queue orders = QueueBuilder.durable("smoke.orders").quorum().build();
        Queue delay = QueueBuilder.durable("smoke.delay.200").quorum()
                .ttl(200)
                .deadLetterExchange("smoke.return")
                .withArgument("x-dead-letter-strategy", "at-least-once")
                .withArgument("x-overflow", "reject-publish")
                .maxLength(1000)
                .build();
        admin.declareExchange(returns);
        admin.declareQueue(orders);
        admin.declareQueue(delay);
        admin.declareBinding(bindByOriginalQueue(returns, "smoke.orders", "smoke.orders", "all-with-x"));

        MessageProperties props = new MessageProperties();
        props.setMessageId("m-1");
        props.setContentType("application/json");
        props.setHeader("x-original-queue", "smoke.orders");
        props.setHeader("x-retry-count", 1);
        props.setHeader("x-first-failed-at", 1_700_000_000_000L);
        props.setHeader("app-header", "kept");
        Message sent = new Message("{\"order\":42}".getBytes(StandardCharsets.UTF_8), props);

        template.invoke(t -> {
            t.send("", "smoke.delay.200", sent);
            t.waitForConfirmsOrDie(5_000);
            return null;
        });

        Message back = template.receive("smoke.orders", 10_000);
        assertThat(back).as("message came back to smoke.orders").isNotNull();
        assertThat(back.getBody()).isEqualTo(sent.getBody());
        MessageProperties got = back.getMessageProperties();
        assertThat(got.getMessageId()).isEqualTo("m-1");
        assertThat(got.getContentType()).isEqualTo("application/json");
        assertThat((Object) got.getHeader("x-original-queue")).isEqualTo("smoke.orders");
        assertThat((Object) got.getHeader("x-retry-count")).isEqualTo(1);
        assertThat((Object) got.getHeader("x-first-failed-at")).isEqualTo(1_700_000_000_000L);
        assertThat((Object) got.getHeader("app-header")).isEqualTo("kept");
    }

    @Test
    void onlyAllWithXRoutesByTheOriginalQueueHeader() {
        HeadersExchange exchange = new HeadersExchange("smoke.match");
        admin.declareExchange(exchange);
        admin.declareQueue(new Queue("smoke.with-x", false));
        admin.declareQueue(new Queue("smoke.plain", false));
        // Both bindings ask for the same header value; only the x-match mode differs.
        admin.declareBinding(bindByOriginalQueue(exchange, "smoke.with-x", "smoke.target", "all-with-x"));
        admin.declareBinding(bindByOriginalQueue(exchange, "smoke.plain", "smoke.target", "all"));

        publish("smoke.target");
        publish("some.other.queue");

        // all-with-x takes only the message meant for it.
        assertThat(admin.getQueueInfo("smoke.with-x").getMessageCount()).isEqualTo(1);
        // Plain all skips x- headers, so the binding has nothing to check and takes every message.
        assertThat(admin.getQueueInfo("smoke.plain").getMessageCount()).isEqualTo(2);
    }

    private static void publish(String originalQueue) {
        MessageProperties props = new MessageProperties();
        props.setHeader("x-original-queue", originalQueue);
        template.invoke(t -> {
            t.send("smoke.match", "", new Message(new byte[0], props));
            t.waitForConfirmsOrDie(5_000);
            return null;
        });
    }

    private static Binding bindByOriginalQueue(HeadersExchange exchange, String queue, String value, String match) {
        return new Binding(queue, Binding.DestinationType.QUEUE, exchange.getName(), "",
                Map.of("x-match", match, "x-original-queue", value));
    }
}

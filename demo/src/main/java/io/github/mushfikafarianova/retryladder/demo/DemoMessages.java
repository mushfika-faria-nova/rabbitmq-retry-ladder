package io.github.mushfikafarianova.retryladder.demo;

import java.nio.charset.StandardCharsets;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** Sends a few messages at startup, so there is something to watch. */
@Component
@ConditionalOnProperty(name = "demo.send-on-startup", havingValue = "true")
public class DemoMessages implements ApplicationRunner {

    private static final Log log = LogFactory.getLog(DemoMessages.class);

    private final RabbitTemplate rabbit;

    public DemoMessages(RabbitTemplate rabbit) {
        this.rabbit = rabbit;
    }

    @Override
    public void run(ApplicationArguments args) {
        // Sent twice on purpose: the second copy shows the duplicate-safe handler skipping it.
        send("orders", "order-1", "{\"order\":1,\"item\":\"book\"}");
        send("orders", "order-1", "{\"order\":1,\"item\":\"book\"}");
        send("payments", "payment-1", "{\"payment\":1,\"amount\":-5}");
        log.info("Sent order-1 (twice) and payment-1. The downstream is down for a while; watch the retries.");
    }

    private void send(String queue, String id, String body) {
        MessageProperties props = new MessageProperties();
        props.setMessageId(id);
        props.setContentType("application/json");
        rabbit.send("", queue, new Message(body.getBytes(StandardCharsets.UTF_8), props));
    }
}

package io.github.mushfikafarianova.retryladder.demo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

import io.github.mushfikafarianova.retryladder.RetryLadder;

/** The app's listeners. Each one runs in manual ack mode and hands its work to RetryLadder. */
@Component
public class Listeners {

    private final RetryLadder retryLadder;
    private final OrderHandler orders;

    public Listeners(RetryLadder retryLadder, OrderHandler orders) {
        this.retryLadder = retryLadder;
        this.orders = orders;
    }

    @RabbitListener(queues = "orders", ackMode = "MANUAL")
    public void onOrder(Message message, Channel channel) throws IOException {
        retryLadder.handle(message, channel, orders::handle);
    }

    @RabbitListener(queues = "payments", ackMode = "MANUAL")
    public void onPayment(Message message, Channel channel) throws IOException {
        retryLadder.handle(message, channel, m -> {
            String body = new String(m.getBody(), StandardCharsets.UTF_8);
            if (body.contains("\"amount\":-")) {
                throw new BadPaymentException("a payment can't have a negative amount: " + body);
            }
        });
    }
}

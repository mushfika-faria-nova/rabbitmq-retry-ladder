package io.github.mushfikafarianova.retryladder;

import org.springframework.amqp.core.Message;

/** The app's own handling of one message. Throwing means it failed. */
@FunctionalInterface
public interface MessageWork {

    void process(Message message) throws Exception;
}

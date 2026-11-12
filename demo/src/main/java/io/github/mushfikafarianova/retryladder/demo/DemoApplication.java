package io.github.mushfikafarianova.retryladder.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * A small app that shows the retries working. Run RabbitMQ with {@code docker compose up -d}, then
 * {@code ./gradlew :demo:bootRun}, and watch the log.
 */
@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}

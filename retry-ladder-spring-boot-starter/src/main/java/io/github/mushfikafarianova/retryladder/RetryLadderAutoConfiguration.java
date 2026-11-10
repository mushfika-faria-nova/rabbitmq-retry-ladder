package io.github.mushfikafarianova.retryladder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/** Checks the retry-ladder settings at startup and logs each queue's retry plan. */
@AutoConfiguration
@ConditionalOnClass(RabbitTemplate.class)
@EnableConfigurationProperties(RetryLadderProperties.class)
public class RetryLadderAutoConfiguration {

    private static final Log log = LogFactory.getLog(RetryLadderAutoConfiguration.class);

    @Bean
    RetryPlans retryLadderPlans(RetryLadderProperties properties) {
        RetryPlans plans = RetryPlans.from(properties, getClass().getClassLoader());
        plans.plans().forEach(plan -> log.info(plan.describe()));
        return plans;
    }
}

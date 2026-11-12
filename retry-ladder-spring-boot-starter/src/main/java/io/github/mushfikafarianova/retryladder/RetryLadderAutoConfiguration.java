package io.github.mushfikafarianova.retryladder;

import java.time.Clock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitConnectionFactoryBean;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.amqp.autoconfigure.CachingConnectionFactoryConfigurer;
import org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration;
import org.springframework.boot.amqp.autoconfigure.RabbitConnectionFactoryBeanConfigurer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Checks the settings at startup and logs each queue's retry plan (R-8), declares the queues and
 * exchanges (02-design.md section 3), and provides the {@link RetryLadder} bean.
 */
@AutoConfiguration(after = RabbitAutoConfiguration.class)
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

    @Bean
    Topology retryLadderNames(RetryLadderProperties properties) {
        return new Topology(properties.getExchangePrefix());
    }

    @Bean
    Declarables retryLadderTopology(Topology names, RetryPlans plans, RetryLadderProperties properties) {
        return new Declarables(names.declarables(plans.plans(), properties.getQueueType()));
    }

    @Bean
    RetryLadder retryLadder(RetryPlans plans, Topology names, RetryLadderProperties properties,
            RetryLadderConnection connection, ObjectProvider<MeterRegistry> meters) {
        return new RetryLadder(plans, names, connection.template(),
                properties.getConfirmTimeout(), meters.getIfAvailable(SimpleMeterRegistry::new), Clock.systemUTC());
    }

    /**
     * The library's own connection, built from the app's spring.rabbitmq settings, with publisher
     * confirms and returns on. Waiting for a confirm never shares a channel with the app's listeners.
     * Not exposed as a ConnectionFactory bean, so Spring Boot's own connection factory stays in place.
     */
    @Bean(destroyMethod = "close")
    RetryLadderConnection retryLadderConnection(RabbitConnectionFactoryBeanConfigurer beanConfigurer,
            CachingConnectionFactoryConfigurer factoryConfigurer) throws Exception {
        RabbitConnectionFactoryBean bean = new RabbitConnectionFactoryBean();
        beanConfigurer.configure(bean);
        bean.afterPropertiesSet();
        CachingConnectionFactory factory = new CachingConnectionFactory(bean.getObject());
        factoryConfigurer.configure(factory);
        factory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
        factory.setPublisherReturns(true);
        factory.setConnectionNameStrategy(cf -> "retry-ladder");
        RabbitTemplate template = new RabbitTemplate(factory);
        template.setMandatory(true);
        return new RetryLadderConnection(factory, template);
    }

    record RetryLadderConnection(CachingConnectionFactory factory, RabbitTemplate template) {
        void close() {
            factory.destroy();
        }
    }
}

package com.message_broker.rabbitmq_consumer.config;


import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.rabbit.stream.config.StreamRabbitListenerContainerFactory;

@Configuration
public class RabbitMqStreamConsumerConfig {


    /**
     * Streams Environment and port
     * */
    @Bean
    public Environment streamEnvironment(){
        return Environment.builder()
                .host("localhost")
                .port(5552)
                .username("guest")
                .password("guest")
                .build();
    }

    //using listener for streams
   /* @Bean
    public StreamRabbitListenerContainerFactory rabbitMqStreamContainerFactory(Environment environment) {
        StreamRabbitListenerContainerFactory factory = new StreamRabbitListenerContainerFactory(environment);
        factory.setNativeListener(true);
        factory.setConsumerCustomizer((id,builder)->{
            builder.name("user-offset-stream-strategy")
                    .offset(OffsetSpecification.first())
                    .manualTrackingStrategy();
        });
        return factory;
    }*/
}

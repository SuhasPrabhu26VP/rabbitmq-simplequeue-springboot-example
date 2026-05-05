package com.message_broker.rabbitmq_producer.config;


import org.springframework.amqp.core.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMqQueueConfig {

    @Value("${spring.rabbitmq.durable.queueName}")
    private String queueName;

    @Value("${spring.rabbitmq.durable.exchangeName}")
    private String directExchangeName;

    @Value("${spring.rabbitmq.durable.routingKey}")
    private String routingKey;


    @Value("${spring.rabbitmq.quorum.queueName}")
    private String quorumQueueName;

    @Value("${spring.rabbitmq.quorum.exchangeName}")
    private String quorumQueueDirectExchangeName;

    @Value("${spring.rabbitmq.quorum.routingKey}")
    private String quorumQueueDirectExchangeRoutingKey;

    //durable queue
    @Bean
    public Queue durableQueue(){
        return new Queue(queueName,true);
    } //second argument true for durable

    @Bean
    public DirectExchange durableQueueDirectExchange(){
        return new DirectExchange(directExchangeName,true,false); // both exchange and queue must be durable else in a crash one of them or both will be lost
    }

    @Bean
    public Binding bindDurableQueueAndExchange(){
        return BindingBuilder.bind(durableQueue()).to(durableQueueDirectExchange()).with(routingKey);
    }


    //quorum queue
    @Bean
    public Queue quorumQueue(){
        return QueueBuilder.durable(quorumQueueName)
                .quorum()
                .build();
    }

    @Bean
    public DirectExchange quorumQueueDirectExchange(){
        return new DirectExchange(quorumQueueDirectExchangeName,true,false); // both exchange and queue must be durable else in a crash one of them or both will be lost
    }

    @Bean
    public Binding bindQuorumQueueAndExchange(){
        return BindingBuilder.bind(quorumQueue()).to(quorumQueueDirectExchange()).with(quorumQueueDirectExchangeRoutingKey);
    }
}

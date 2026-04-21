package com.message_broker.rabbitmq_producer.config;




import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMqProducerConfig {

    @Value("${spring.rabbitmq.queueName}")
    private String queueName;

    @Value("${spring.rabbitmq.exchangeName}")
    private String directExchangeName;

    @Value("${spring.rabbitmq.routingKey}")
    private String routingKey;

    @Bean
    public Queue queue(){
        return new Queue(queueName);
    }

    @Bean
    public DirectExchange exchange(){
        return new DirectExchange(directExchangeName);
    }


    @Bean
    public Binding binding(){
        return BindingBuilder.bind(queue()).to(exchange()).with(routingKey);
    }

    @Bean
    public MessageConverter converter(){return new JacksonJsonMessageConverter();}


}

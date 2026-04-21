package com.message_broker.rabbitmq_producer;

import com.message_broker.rabbitmq_producer.dto.UserDetailsDTO;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class RabbitMqProducer {

    @Value("${spring.rabbitmq.exchangeName}")
    private String directExchangeName;

    @Value("${spring.rabbitmq.routingKey}")
    private String routingKey;

    private RabbitTemplate rabbitTemplate;

    public RabbitMqProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void sendMesssage(final UserDetailsDTO userDetailsDTO){
        rabbitTemplate.convertAndSend(directExchangeName,routingKey,userDetailsDTO);
    }
}

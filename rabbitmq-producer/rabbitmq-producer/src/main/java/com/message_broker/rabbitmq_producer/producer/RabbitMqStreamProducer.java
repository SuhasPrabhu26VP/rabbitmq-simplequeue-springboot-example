package com.message_broker.rabbitmq_producer.producer;


import com.message_broker.rabbitmq_producer.dto.UserDetailsDTO;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;
import org.springframework.stereotype.Service;

@Service
public class RabbitMqStreamProducer {
    private final RabbitStreamTemplate userStreamTemplate;
    private final RabbitStreamTemplate orderStreamTemplate;

    public RabbitMqStreamProducer(
            @Qualifier("userStreamTemplate") RabbitStreamTemplate userStreamTemplate,
            @Qualifier("orderStreamTemplate") RabbitStreamTemplate orderStreamTemplate) {
        this.userStreamTemplate = userStreamTemplate;
        this.orderStreamTemplate = orderStreamTemplate;
    }

    public void publishUser(UserDetailsDTO payload) {
        userStreamTemplate.convertAndSend(payload);
    }

    public void publishOrder(UserDetailsDTO payload) {
        orderStreamTemplate.convertAndSend(payload);
    }
}

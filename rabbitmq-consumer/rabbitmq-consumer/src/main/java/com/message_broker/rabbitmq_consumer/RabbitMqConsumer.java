package com.message_broker.rabbitmq_consumer;

import com.message_broker.rabbitmq_consumer.dto.UserDetailsDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class RabbitMqConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqConsumer.class);


    @RabbitListener(queues = {"${spring.rabbitmq.queueName}"})
    public void consume(UserDetailsDTO dto, Message message){
        LOGGER.info(String.format("Received message -> %s", dto));
    }
}

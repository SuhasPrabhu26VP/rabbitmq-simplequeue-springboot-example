package com.message_broker.rabbitmq_producer.producer;

import com.message_broker.rabbitmq_producer.dto.UserDetailsDTO;
import com.rabbitmq.stream.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
public class SuperStreamProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SuperStreamProducer.class);

    private final RabbitStreamTemplate superStreamTemplate;

    public SuperStreamProducer(RabbitStreamTemplate superStreamTemplate) {
        this.superStreamTemplate = superStreamTemplate;
    }

    public void publish(String customerId, String payload) {


        Message message = superStreamTemplate.messageBuilder()
                .applicationProperties()
                .entry("customerId", customerId)    // routing key
                .messageBuilder()
                .addData(payload.getBytes())
                .build();

        superStreamTemplate.send(message);
    }
    
    public void publishWithKey(UserDetailsDTO payload)  {
        String json = new ObjectMapper().writeValueAsString(payload);
        publish(payload.id().toString(), json);
    }
}

package com.message_broker.rabbitmq_producer.producer;


import com.message_broker.rabbitmq_producer.dto.AddressDTO;
import com.message_broker.rabbitmq_producer.dto.UserDetailsDTO;
import com.rabbitmq.stream.Message;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
public class RabbitMqStreamProducer {
    private final RabbitStreamTemplate userStreamTemplate;
    private final ObjectMapper mapper;
  //  private final RabbitStreamTemplate orderStreamTemplate;

    public RabbitMqStreamProducer(
            @Qualifier("userStreamTemplate") RabbitStreamTemplate userStreamTemplate, ObjectMapper mapper
            /*@Qualifier("orderStreamTemplate") RabbitStreamTemplate orderStreamTemplate*/) {
        this.userStreamTemplate = userStreamTemplate;
     //   this.orderStreamTemplate = orderStreamTemplate;
        this.mapper = mapper;
    }

    public void publishUser(UserDetailsDTO payload) {
        userStreamTemplate.convertAndSend(payload);
    }

    public void publishUserInfoWithAddress(UserDetailsDTO payload) {
        Message message = userStreamTemplate
                .messageBuilder()
                .addData(mapper.writeValueAsBytes(payload))
                .messageAnnotations().entry("x-stream-filter-value", "userInfoWithAddress")
                .messageBuilder().build();
        userStreamTemplate.send(message);
    }

    public void publishUserInfoWithAddress(AddressDTO payload) {
        Message message = userStreamTemplate
                .messageBuilder()
                .addData(mapper.writeValueAsBytes(payload))
                .messageAnnotations().entry("x-stream-filter-value", "address")
                .messageBuilder().build();
        userStreamTemplate.send(message);
    }
}

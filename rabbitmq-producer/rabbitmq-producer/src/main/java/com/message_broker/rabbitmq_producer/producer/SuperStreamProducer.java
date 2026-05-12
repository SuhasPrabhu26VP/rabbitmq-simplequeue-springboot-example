package com.message_broker.rabbitmq_producer.producer;

import com.message_broker.rabbitmq_producer.dto.AddressDTO;
import com.message_broker.rabbitmq_producer.dto.UserDetailsDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;
import org.springframework.rabbit.stream.support.StreamMessageProperties;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
public class SuperStreamProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SuperStreamProducer.class);

    @Autowired
    @Qualifier("userSuperStreamTemplate")
    private RabbitStreamTemplate userSuperStreamTemplate;

    @Autowired
    @Qualifier("userSuperStreamWithHashTemplate")
    private RabbitStreamTemplate userSuperStreamWithHashTemplate;


    public void publishUserInfoWithKey(UserDetailsDTO payload) {
       /* StreamMessageProperties props = new StreamMessageProperties();
        props.setHeader("routingKey", "userSuperStreamRoutingKey");
        props.setHeader("x-stream-filter-value", "userInfoWithAddress");*/
       // Message message = userSuperStreamTemplate.streamMessageConverter().toMessage(payload,props);
        userSuperStreamTemplate.convertAndSend(payload, message -> {
            message.getMessageProperties().setHeader("routingKey", "userSuperStreamRoutingKey");
            message.getMessageProperties().setHeader("x-stream-filter-value", "userInfoWithAddress");
            return message;
        });
       // userSuperStreamTemplate.send(message);
    }

    public void publishUserAddressInfoWithKey(AddressDTO payload) {
        /*StreamMessageProperties props = new StreamMessageProperties();
        props.setHeader("routingKey", "addressSuperStreamRoutingKey");
        props.setHeader("x-stream-filter-value", "address");
        Message message = userSuperStreamTemplate.streamMessageConverter().toMessage(payload,props);
        userSuperStreamTemplate.send(message);*/
        userSuperStreamTemplate.convertAndSend(payload, message -> {
            message.getMessageProperties().setHeader("routingKey", "addressSuperStreamRoutingKey");
            message.getMessageProperties().setHeader("x-stream-filter-value", "address");
            return message;
        });
    }
    public void publishUserInfoWithHash(UserDetailsDTO payload) {
        /*StreamMessageProperties props = new StreamMessageProperties();
        props.setHeader("userId", payload.id());
        props.setHeader("x-stream-filter-value", "userInfoWithAddress");
        Message message = userSuperStreamWithHashTemplate.streamMessageConverter()
                .toMessage(payload, props);

        userSuperStreamWithHashTemplate.send(message);*/

        userSuperStreamWithHashTemplate.convertAndSend(payload, message -> {
            message.getMessageProperties().setHeader("userId", payload.id());
            message.getMessageProperties().setHeader("x-stream-filter-value", "userInfoWithAddress");
            return message;
        });
    }

    public void publishAddressWithHash(AddressDTO payload) {
       /* StreamMessageProperties props = new StreamMessageProperties();
        props.setHeader("userId", payload.userId());
        props.setHeader("x-stream-filter-value", "address");
        Message message = userSuperStreamWithHashTemplate.streamMessageConverter()
                .toMessage(payload, props);
        userSuperStreamWithHashTemplate.send(message);*/

        userSuperStreamWithHashTemplate.convertAndSend(payload, message -> {
            message.getMessageProperties().setHeader("userId", payload.userId());
            message.getMessageProperties().setHeader("x-stream-filter-value", "address");
            return message;
        });
    }
}

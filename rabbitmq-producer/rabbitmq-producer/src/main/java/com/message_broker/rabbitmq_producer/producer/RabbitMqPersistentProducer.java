package com.message_broker.rabbitmq_producer.producer;

import com.message_broker.rabbitmq_producer.dto.UserDetailsDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class RabbitMqPersistentProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqPersistentProducer.class);



    @Value("${spring.rabbitmq.durable.exchangeName}")
    private String directExchangeName;

    @Value("${spring.rabbitmq.durable.routingKey}")
    private String routingKey;

    @Value("${spring.rabbitmq.quorum.exchangeName}")
    private String quorumDirectExchangeName;

    @Value("${spring.rabbitmq.quorum.routingKey}")
    private String quorumRoutingKey;

    private final RabbitTemplate rabbitTemplate;

    public RabbitMqPersistentProducer(final RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void sendPersistedUserDetailstoDurableQueue(UserDetailsDTO userDetailsDTO) {
        MessageProperties props = new MessageProperties();
        props.setDeliveryMode(MessageDeliveryMode.PERSISTENT); // write to disk

        Message message = new JacksonJsonMessageConverter()
                .toMessage(userDetailsDTO, props);

        rabbitTemplate.send(directExchangeName, routingKey, message);
    }


    public void sendPersistedUserDetailstoQuorumQueue(UserDetailsDTO userDetailsDTO) {

        rabbitTemplate.convertAndSend(quorumDirectExchangeName, quorumRoutingKey, userDetailsDTO);
    }


}

package com.message_broker.rabbitmq_consumer;

import com.message_broker.rabbitmq_consumer.dto.UserDetailsDTO;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class RabbitMqNativeStreamConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqNativeStreamConsumer.class);

    private final Environment streamEnvironment;

    public RabbitMqNativeStreamConsumer(Environment streamEnvironment) {
        this.streamEnvironment = streamEnvironment;
    }

    @PostConstruct
    public void start(){
        streamEnvironment.consumerBuilder()
                .stream("userStream")
                .offset(OffsetSpecification.first())
                .messageHandler((context, message) -> {
                    UserDetailsDTO userDetailsDTO = new ObjectMapper().convertValue(message.getBodyAsBinary(),UserDetailsDTO.class);
                    LOGGER.info("received message from stream {}",userDetailsDTO);
                }).build();
    }
}

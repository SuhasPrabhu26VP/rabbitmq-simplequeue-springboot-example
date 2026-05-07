package com.message_broker.rabbitmq_consumer;

import com.message_broker.rabbitmq_consumer.dto.UserDetailsDTO;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class ServerSideOffsetConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerSideOffsetConsumer.class);

    private Environment streamEnvironment;

    private Consumer consumer;

    private ObjectMapper mapper;

    public ServerSideOffsetConsumer(Environment streamEnvironment,Consumer consumer, ObjectMapper mapper) {
        this.streamEnvironment = streamEnvironment;
        this.consumer = consumer;
        this.mapper = mapper;
    }


    @PostConstruct
    public void start() {
        this.consumer = streamEnvironment.consumerBuilder()
                .stream("userstream")
                .name("user-consumer-group")
                .offset(OffsetSpecification.first())
                .manualTrackingStrategy()
                .builder()
                .messageHandler((context, message) -> {
                    try {
                        UserDetailsDTO payload = mapper.readValue(message.getBodyAsBinary(),UserDetailsDTO.class);
                        LOGGER.info("Received: " + payload
                                + " at offset: " + context.offset());
                        context.storeOffset();
                        if(payload.tracker().equalsIgnoreCase("END")) {
                            context.consumer().close();
                        }

                    } catch (Exception e) {
                        LOGGER.info("Failed: " + e.getMessage());
                    }
                })
                .build();
    }

    @PreDestroy
    public void stop() {
        if (consumer != null) consumer.close();
    }
}

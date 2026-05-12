package com.message_broker.rabbitmq_consumer;

import com.message_broker.rabbitmq_consumer.dto.AddressDTO;
import com.message_broker.rabbitmq_consumer.dto.UserDetailsDTO;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class RabbitMqNativeStreamConsumerWIthFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqNativeStreamConsumerWIthFilter.class);

    private final Environment streamEnvironment;
    private Consumer consumer;
    private ObjectMapper objectMapper;

    public RabbitMqNativeStreamConsumerWIthFilter(Environment streamEnvironment) {
        this.streamEnvironment = streamEnvironment;
    }

    @PostConstruct
    public void start() {
        this.consumer = streamEnvironment.consumerBuilder()
                .stream("userstream")
                // This Bloom filter will be evaluated server-side per chunk (Stage 1).
                    .filter()
                    .values("userInfoWithAddress", "address") //tell server to pick the chunk
                    .postFilter(message -> {                    // tell consumer which chunk to process
                    String filterValue = (String) message.getApplicationProperties()
                            .get("x-stream-filter-value");
                    return "userInfoWithAddress".equals(filterValue)
                            || "address".equals(filterValue);
                })
                    .matchUnfiltered(true)
                .builder()
                .messageHandler((context, message) -> {
                    try {
                        String filterValue = (String) message.getMessageAnnotations().get("x-stream-filter-value");
                        // This filter will be evaluted client-side per message (Stage 3).
                            if("address".equals(filterValue)){
                                UserDetailsDTO payload = objectMapper.readValue(message.getBodyAsBinary(), UserDetailsDTO.class);
                                LOGGER.info("Payload: {}", payload);
                            }
                            if("userInfoWithAddress".equals(filterValue)){
                                AddressDTO payload = objectMapper.readValue(message.getBodyAsBinary(), AddressDTO.class);
                                LOGGER.info("Payload: {}", payload);
                            }
                        context.processed();
                    } catch (Exception e) {
                        LOGGER.error("Failed at offset {}: {}", context.offset(), e.getMessage());
                    }
                })
                .build();
    }

    @PreDestroy
    public void stop() {
        if (consumer != null) consumer.close();
    }
}
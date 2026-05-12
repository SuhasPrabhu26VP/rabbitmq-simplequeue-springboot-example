package com.message_broker.rabbitmq_consumer;

import com.message_broker.rabbitmq_consumer.dto.AddressDTO;
import com.message_broker.rabbitmq_consumer.dto.UserDetailsDTO;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class RabbitMqNativeSuperStreamConsumerWithHashAndFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqNativeSuperStreamConsumerWithHashAndFilter.class);

    private final Environment streamEnvironment;
    private Consumer consumer;

    @Autowired
    private ObjectMapper objectMapper;

    public RabbitMqNativeSuperStreamConsumerWithHashAndFilter(Environment streamEnvironment) {
        this.streamEnvironment = streamEnvironment;
    }


    @PostConstruct
    public void start() {
        this.consumer = streamEnvironment.consumerBuilder()
                .superStream("userSuperStreamWithHash")
                .singleActiveConsumer()
                .name("userSuperStream-consumer-group")
                .filter()
                .values("userInfoWithAddress", "address")
                .postFilter(message -> {                    // tell consumer which chunk to process
                    String filterValue = (String) message.getApplicationProperties()
                            .get("x-stream-filter-value");
                    return "userInfoWithAddress".equals(filterValue)
                            || "address".equals(filterValue);
                })
                .matchUnfiltered(true)
                .builder()
                .consumerUpdateListener(context -> {
                    LOGGER.info("Partition changed - stream: {}, active: {}",
                            context.stream(), context.isActive());
                    return OffsetSpecification.first();
                })
                .messageHandler((context, message) -> {
                    try {
                        String filterValue = (String) message.getApplicationProperties()
                                .get("x-stream-filter-value");
                        if ("userInfoWithAddress".equals(filterValue)) {
                            UserDetailsDTO payload = objectMapper.readValue(
                                    message.getBodyAsBinary(), UserDetailsDTO.class);
                            LOGGER.info("Partition: {} | UserDetails: {}", context.stream(), payload);

                        } else if ("address".equals(filterValue)) {
                            AddressDTO payload = objectMapper.readValue(
                                    message.getBodyAsBinary(), AddressDTO.class);
                            LOGGER.info("Partition: {} | Address: {}", context.stream(), payload);
                        }
                        context.storeOffset();
                        context.processed();
                    } catch (Exception e) {
                        LOGGER.error("Failed at offset {} on partition {}: {}",
                                context.offset(), context.stream(), e.getMessage());
                    }
                })
                .build();
    }


    @PreDestroy
    public void stop() {
        if (consumer != null) consumer.close();
    }
}

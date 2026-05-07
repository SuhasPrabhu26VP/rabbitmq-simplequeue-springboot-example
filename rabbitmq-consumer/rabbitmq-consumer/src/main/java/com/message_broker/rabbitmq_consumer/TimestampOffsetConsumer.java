package com.message_broker.rabbitmq_consumer;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Component
public class TimestampOffsetConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimestampOffsetConsumer.class);

    private final Environment streamEnvironment;
    private Consumer consumer;

    public TimestampOffsetConsumer(Environment streamEnvironment) {
        this.streamEnvironment = streamEnvironment;
    }

    @PostConstruct
    public void start() {

        long minutesAgo = System.currentTimeMillis() - Duration.ofMinutes(2).toMillis();

        this.consumer = streamEnvironment.consumerBuilder()
                .stream("userstream")
                .offset(OffsetSpecification.timestamp(minutesAgo))
                .messageHandler((context, message) -> {
                    try {
                        String payload = new String(message.getBodyAsBinary());

                        LOGGER.info("Offset: {} | Payload: {}", context.offset(), payload);

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

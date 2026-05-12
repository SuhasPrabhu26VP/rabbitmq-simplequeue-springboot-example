package com.message_broker.rabbitmq_producer.config;

import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.Environment;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMqSuperStreamConfig {

    @Autowired
    @Qualifier("streamEnvironment")
    private Environment streamEnvironment;

    @PostConstruct
    public void createSuperStreams() {
        // Key-based
        streamEnvironment.streamCreator()
                .name("userSuperStreamWithRoutingKey")
                .maxLengthBytes(ByteCapacity.GB(5))
                .maxSegmentSizeBytes(ByteCapacity.MB(500))
                .superStream()
                .bindingKeys("userSuperStreamRoutingKey", "addressSuperStreamRoutingKey")
                .creator()
                .create();

        // Hash-based
        streamEnvironment.streamCreator()
                .name("userSuperStreamWithHash")
                .maxLengthBytes(ByteCapacity.GB(5))
                .maxSegmentSizeBytes(ByteCapacity.MB(500))
                .superStream()
                .partitions(3)
                .creator()
                .create();
    }


}

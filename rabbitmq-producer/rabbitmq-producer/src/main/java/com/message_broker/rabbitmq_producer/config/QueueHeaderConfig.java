package com.message_broker.rabbitmq_producer.config;

import java.util.Map;

public record QueueHeaderConfig(
        String queueName,
        String exchangeName,
        String routingKey,
        String region,
        String priority,
        HeaderMatchStrategy strategy,
        Boolean durable,
        Boolean exclusive,
        Boolean autoDelete
) {}

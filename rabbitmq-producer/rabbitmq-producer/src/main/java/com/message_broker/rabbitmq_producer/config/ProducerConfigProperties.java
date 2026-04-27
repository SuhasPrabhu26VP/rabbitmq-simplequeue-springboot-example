package com.message_broker.rabbitmq_producer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Getter @Setter
@ConfigurationProperties(prefix = "spring.rabbitmq")
@Configuration("producerConfigProperties")
public class ProducerConfigProperties {


    private Map<String, HeaderQueueProperties> header;
    private Map<String, TopicQueueProperties> topic;
    private Map<String, FanoutQueueProperties> fanout;

    @Getter @Setter
    public static class HeaderQueueProperties extends BaseQueueProperties {
        private String strategy;   // ALL / ANY / SINGLE
        private String region;
        private String priority;
    }

    @Getter @Setter
    public static class TopicQueueProperties extends BaseQueueProperties{
        private String routingKey;
    }

    @Getter @Setter
    public static class FanoutQueueProperties extends BaseQueueProperties{
    }


    @Getter @Setter
    public static class BaseQueueProperties{
        private String queueName;
        private String exchangeName;
        private Boolean durable;
        private Boolean exclusive;
        private Boolean autoDelete;
    }



}

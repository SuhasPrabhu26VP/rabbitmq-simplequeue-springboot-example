package com.message_broker.rabbitmq_producer;

import com.message_broker.rabbitmq_producer.config.RabbitMqProducerConfig;
import com.message_broker.rabbitmq_producer.dto.UserDetailsDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class RabbitMqProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqProducerConfig.class);

    @Value("${spring.rabbitmq.exchangeName}")
    private String directExchangeName;

    @Value("${spring.rabbitmq.routingKey}")
    private String routingKey;

    private RabbitTemplate rabbitTemplate;

    public RabbitMqProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    /**
     *
     * No Correlated data no callback
     * */

    public void sendMesssage(final UserDetailsDTO userDetailsDTO){
        rabbitTemplate.convertAndSend(directExchangeName,routingKey,userDetailsDTO);
    }


    /**
     *
     * Aync global callback used along with setCallBack in template
     * */
    public void sendCriticalGlobalAsyncMessage(final UserDetailsDTO userDetailsDTO) {
        CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
        rabbitTemplate.convertAndSend(directExchangeName, routingKey, userDetailsDTO, correlationData);
    }


    /**
     *
     * Asyn per message callback
     * */
    public void sendCriticalMessage(final UserDetailsDTO userDetailsDTO) {
        CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());

        correlationData.getFuture().whenComplete((confirm, ex) -> {
            if (ex != null || !confirm.ack()) {
                LOGGER.error("Critical message failed, id: {}", correlationData.getId());
                // retry or save to outbox
            } else {
                LOGGER.info("Critical message confirmed, id: {}", correlationData.getId());
            }
        });

        rabbitTemplate.convertAndSend(directExchangeName, routingKey, userDetailsDTO, correlationData);
    }

    /**
     * SYNC Per Message callback
     * */
    public void sendCriticalSyncMessage(final UserDetailsDTO userDetailsDTO) {
        rabbitTemplate.invoke(operations -> {
            operations.convertAndSend(directExchangeName, routingKey, userDetailsDTO);
            operations.waitForConfirmsOrDie(5000);
            return null;
        });
    }


    /**
     * Durable Message with DLQ functionality
     * */
    public void sendMessageToRabbitMq(final UserDetailsDTO userDetailsDTO) {
        rabbitTemplate.convertAndSend("mainQueueExchange","mainQueueExchangeRoutingKey",userDetailsDTO);
    }


    public void sendHeaderMesssageToEveryone(final UserDetailsDTO userDetailsDTO){
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setHeader("IN","HIGH");
        messageProperties.setHeader("EU","LOW");
        messageProperties.setHeader("US","MEDIUM");
        Message message = rabbitTemplate.getMessageConverter().toMessage(userDetailsDTO,messageProperties);
        rabbitTemplate.send(message);
    }

    public void sendHeaderMesssageToAtlantic(final UserDetailsDTO userDetailsDTO){
        MessageProperties messageProperties = new MessageProperties();;
        messageProperties.setHeader("EU","LOW");
        messageProperties.setHeader("US","MEDIUM");
        Message message = rabbitTemplate.getMessageConverter().toMessage(userDetailsDTO,messageProperties);
        rabbitTemplate.send(message);
    }

    public void sendHeaderMesssageIndia(final UserDetailsDTO userDetailsDTO){
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setHeader("IN","HIGH");
        Message message = rabbitTemplate.getMessageConverter().toMessage(userDetailsDTO,messageProperties);
        rabbitTemplate.send(message);
    }
}

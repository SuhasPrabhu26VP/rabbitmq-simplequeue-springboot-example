package com.message_broker.rabbitmq_consumer;

import com.message_broker.rabbitmq_consumer.dto.UserDetailsDTO;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpTimeoutException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

import javax.management.BadAttributeValueExpException;
import java.util.InvalidPropertiesFormatException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class RabbitMqConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqConsumer.class);


    //testing quorum queue logic
    @RabbitListener(queues = {"quorumQueueTest"})
    public void consumeFromQuorumQueue(UserDetailsDTO dto, Message message) throws Exception {
        LOGGER.info(String.format("Received message -> %s", dto));
    }

    //testing durable queue logic
    @RabbitListener(queues = {"classicQueueTestWithPersistence"})
    public void consumeFromDurableQueue(UserDetailsDTO dto, Message message) throws Exception {
        LOGGER.info(String.format("Received message -> %s", dto));
    }

    //testing retry logic
 //   @RabbitListener(queues = {"${spring.rabbitmq.queueName}"} , containerFactory = "rabbitListenerContainerFactoryWithRetry")
    public void consume(UserDetailsDTO dto, Message message) throws Exception {
        LOGGER.info(String.format("Received message -> %s", dto));
        throw new Exception("Message invalid");
    }

 //   @RabbitListener(queues = {"${spring.rabbitmq.queueName}"}, containerFactory = "listenerContainerFactory")
    public void consume(UserDetailsDTO userDetailsDTO, Message message, Channel channel) throws Exception {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        long now = System.currentTimeMillis();
        try {
            if ((now / 10000) % 2 == 0) {
                throw new AmqpTimeoutException("Consumer unavailable");
            }
            if (userDetailsDTO.firstName().length() <= 3) {
                LOGGER.info(String.format("Received message is corrupted-> %s", userDetailsDTO));
                throw new InvalidPropertiesFormatException("Message is corrupted");
            }
            Pattern p = Pattern.compile(
                    "[^A-Za-z]", Pattern.CASE_INSENSITIVE);

            Matcher m = p.matcher(userDetailsDTO.firstName());
            Matcher m2 = p.matcher(userDetailsDTO.lastName());

            boolean res = m.find();
            boolean res2 = m2.find();
            if(res || res2){
                throw new BadAttributeValueExpException("Invalid Value");
            }
            channel.basicAck(deliveryTag, false);
            /**
             * ACK the message
             * delivery tag
             * multiple==false
             */
        } catch (AmqpTimeoutException amqpTimeoutException){
            LOGGER.info(String.format("Received message is Not Ack due to server down-> %s", userDetailsDTO));
            channel.basicNack(deliveryTag, false, true);
            /**
             * NOT ACK the Message but Requeue
             * delivery tag
             * multiple==false
             * requeue==true
             */
        } catch (InvalidPropertiesFormatException e) {
            LOGGER.info(String.format("Received message is Not Ack-> %s", userDetailsDTO));
            channel.basicNack(deliveryTag, false, false);
            /**
             * Not Acknowledge the message dont requeue
             * delivery tag
             * multiple==false
             * requeue==false
             */
        }catch (BadAttributeValueExpException invalidException){
            LOGGER.info(String.format("Received message is Not Ack-> %s", userDetailsDTO));
            channel.basicReject(deliveryTag,  false);
            /**
             * reject the message
             * delivery tag
             * requeue==false
             */
        }catch (Exception e){
            LOGGER.info(String.format("Received message is Not Ack-> %s", userDetailsDTO));
            channel.basicNack(deliveryTag,  false,true);
            /**
             * reject the message
             * delivery tag
             * requeue==true
             */
        }
    }
}

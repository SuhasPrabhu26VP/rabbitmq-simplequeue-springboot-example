package com.message_broker.rabbitmq_producer.config;

import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.Environment;

import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.rabbit.stream.config.SuperStream;
import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;
import org.springframework.rabbit.stream.support.StreamAdmin;

import java.time.Duration;

@Configuration
public class RabbitMqStreamsConfig {

    /**
     * Streams Environment and port
     * */
    @Bean
    @Qualifier("streamEnvironment")
    public Environment streamEnvironment(){
        return Environment.builder()
                .host("localhost")
                .port(5552)
                .username("guest")
                .password("guest")
                .build();
    }

    //alternate way to implement streams via AMQP Protocol
   /* @Bean
    public Queue eventsStream() {
        return QueueBuilder
                .durable("userstreamViaAMQP")
                .withArgument("x-queue-type", "stream")
                .withArgument("x-max-length-bytes",
                        10L * 1024 * 1024 * 1024)
                .withArgument("x-max-age", "7D")
                .withArgument("x-stream-max-segment-size-bytes",
                        100 * 1024 * 1024)
                .build();
    }*/

    /**
     * Stream Admin to create streams
     * */
    @Bean
    StreamAdmin streamAdmin(@Qualifier("streamEnvironment") final Environment streamEnvironment){
        return  new StreamAdmin(streamEnvironment,sc->{
            sc.stream("userstream")
                    .maxAge(Duration.ofDays(2))
                    .maxLengthBytes(ByteCapacity.B(10L*1024*1024*1024))
                    .create();
           /* sc.stream("orderstream")
                    .maxAge(Duration.ofDays(7))
                    .maxLengthBytes(ByteCapacity.B(10L * 1024 * 1024 * 1024))
                    .create();*/
        });
    }





    //alternative way to use via AMQP Protocol

   /* @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }*/


    /**
    *
    * use created stream in a stream template
    * */
    @Bean("userStreamTemplate")
    public RabbitStreamTemplate streamTemplate(Environment streamEnvironment) {
        RabbitStreamTemplate template = new RabbitStreamTemplate(streamEnvironment, "userstream");
        template.setMessageConverter(new JacksonJsonMessageConverter());
        return template;
    }

    @Bean("userSuperStreamTemplate")
    public RabbitStreamTemplate userSuperStreamTemplate(
            @Qualifier("streamEnvironment") Environment streamEnvironment) {

        RabbitStreamTemplate template = new RabbitStreamTemplate(
                streamEnvironment, "userSuperStreamWithRoutingKey");

        template.setSuperStreamRouting(message ->
                message.getApplicationProperties().get("routingKey").toString());

        template.setMessageConverter(new JacksonJsonMessageConverter());
        return template;
    }

    @Bean("userSuperStreamWithHashTemplate")
    public RabbitStreamTemplate userSuperStreamWithHashTemplate(
            @Qualifier("streamEnvironment") Environment streamEnvironment) {

        RabbitStreamTemplate template = new RabbitStreamTemplate(
                streamEnvironment, "userSuperStreamWithHash");


        template.setSuperStreamRouting(message ->
                message.getApplicationProperties().get("userId").toString());

        template.setMessageConverter(new JacksonJsonMessageConverter());
        return template;
    }

/*    @Bean
    public RabbitStreamTemplate orderStreamTemplate(Environment streamEnvironment) {
        return new RabbitStreamTemplate(streamEnvironment, "orderstream");
    }*/

/*    @Bean
    SuperStream superStream() {
        return new SuperStream("my.super.stream", 3);
    }*/

   /* @Bean
    public RabbitStreamTemplate superStreamTemplate(Environment env) {
        RabbitStreamTemplate template = new RabbitStreamTemplate(
                env, "my.super.stream"
        );
        template.setProducerCustomizer((name, builder) ->
                builder.routing(message ->
                        message.getApplicationProperties()
                                .get("customerId")
                                .toString()
                )
        );

        return template;
    }*/

}

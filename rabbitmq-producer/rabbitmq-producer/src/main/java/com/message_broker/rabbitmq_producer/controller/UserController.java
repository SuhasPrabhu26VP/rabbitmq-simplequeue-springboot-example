package com.message_broker.rabbitmq_producer.controller;

import com.message_broker.rabbitmq_producer.RabbitMqProducer;
import com.message_broker.rabbitmq_producer.dto.UserDetailsDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Description;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class UserController {

    @Autowired
    public RabbitMqProducer rabbitMqProducer;

    @PostMapping("/user")
    @Description("Direct Exchange")
    public ResponseEntity<String> sendUserInfo(@RequestBody UserDetailsDTO userDetailsDTO){
        rabbitMqProducer.sendMesssage(userDetailsDTO);
        return  ResponseEntity.ok("Message Sent");
    }

    @PostMapping("/user/fanout")
    @Description("fanout Exchange")
    public ResponseEntity<String> sendUserInfoFanout(@RequestBody UserDetailsDTO userDetailsDTO){
        rabbitMqProducer.sendMesssage(userDetailsDTO);
        return  ResponseEntity.ok("Message Sent");
    }

    @PostMapping("/user/topic")
    @Description("topic Exchange")
    public ResponseEntity<String> sendUserInfoTopic(@RequestBody UserDetailsDTO userDetailsDTO){
        rabbitMqProducer.sendMesssage(userDetailsDTO);
        return  ResponseEntity.ok("Message Sent");
    }

    @PostMapping("/user/header")
    @Description("header Exchange")
    public ResponseEntity<String> sendUserInfoHeader(@RequestBody UserDetailsDTO userDetailsDTO){
        rabbitMqProducer.sendMesssage(userDetailsDTO);
        return  ResponseEntity.ok("Message Sent");
    }
}

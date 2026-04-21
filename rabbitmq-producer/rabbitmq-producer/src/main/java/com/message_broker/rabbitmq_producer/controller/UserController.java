package com.message_broker.rabbitmq_producer.controller;

import com.message_broker.rabbitmq_producer.RabbitMqProducer;
import com.message_broker.rabbitmq_producer.dto.UserDetailsDTO;
import org.springframework.beans.factory.annotation.Autowired;
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
    public ResponseEntity<String> sendUserInfo(@RequestBody UserDetailsDTO userDetailsDTO){
        rabbitMqProducer.sendMesssage(userDetailsDTO);
        return  ResponseEntity.ok("Message Sent");
    }
}

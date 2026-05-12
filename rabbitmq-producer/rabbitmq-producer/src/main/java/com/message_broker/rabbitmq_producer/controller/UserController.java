package com.message_broker.rabbitmq_producer.controller;

import com.message_broker.rabbitmq_producer.dto.AddressDTO;
import com.message_broker.rabbitmq_producer.producer.RabbitMqPersistentProducer;
import com.message_broker.rabbitmq_producer.producer.RabbitMqProducer;
import com.message_broker.rabbitmq_producer.dto.UserDetailsDTO;
import com.message_broker.rabbitmq_producer.producer.RabbitMqStreamProducer;
import com.message_broker.rabbitmq_producer.producer.SuperStreamProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Description;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1")
public class UserController {

    @Autowired
    public RabbitMqProducer rabbitMqProducer;

    @Autowired
    public RabbitMqPersistentProducer persistentProducer;


    @Autowired
    public RabbitMqStreamProducer rabbitMqStreamProducer;

    @Autowired
    public SuperStreamProducer superStreamProducer;

    @PostMapping("/user")
    @Description("Direct Exchange")
    public ResponseEntity<String> sendUserInfo(@RequestBody UserDetailsDTO userDetailsDTO) {
        rabbitMqProducer.sendMesssage(userDetailsDTO);
        return ResponseEntity.ok("Message Sent");
    }

    @PostMapping("/user/fanout")
    @Description("fanout Exchange")
    public ResponseEntity<String> sendUserInfoFanout(@RequestBody UserDetailsDTO userDetailsDTO) {
        rabbitMqProducer.sendMesssage(userDetailsDTO);
        return ResponseEntity.ok("Message Sent");
    }

    @PostMapping("/user/topic")
    @Description("topic Exchange")
    public ResponseEntity<String> sendUserInfoTopic(@RequestBody UserDetailsDTO userDetailsDTO) {
        rabbitMqProducer.sendMesssage(userDetailsDTO);
        return ResponseEntity.ok("Message Sent");
    }

    @PostMapping("/user/header")
    @Description("header Exchange")
    public ResponseEntity<String> sendUserInfoHeader(@RequestBody UserDetailsDTO userDetailsDTO) {
        rabbitMqProducer.sendMesssage(userDetailsDTO);
        return ResponseEntity.ok("Message Sent");
    }

    @PostMapping("/user/durable")
    @Description("Durable Queue")
    public ResponseEntity<String> sendUserInfoToDurable(@RequestBody UserDetailsDTO userDetailsDTO) {
        persistentProducer.sendPersistedUserDetailstoDurableQueue(userDetailsDTO);
        return ResponseEntity.ok("Message Sent");
    }

    @PostMapping("/user/quorum")
    @Description("Quorum Queue")
    public ResponseEntity<String> sendUserInfoToQuorum(@RequestBody UserDetailsDTO userDetailsDTO) {
        persistentProducer.sendPersistedUserDetailstoQuorumQueue(userDetailsDTO);
        return ResponseEntity.ok("Message Sent");
    }


    @PostMapping("/user/stream")
    @Description("stream Queue")
    public ResponseEntity<String> sendUserInfoToStream(@RequestBody UserDetailsDTO userDetailsDTO) {
        rabbitMqStreamProducer.publishUser(userDetailsDTO);
        return ResponseEntity.ok("Message Sent");
    }

    @PostMapping("/user/super/stream")
    @Description("super Queue")
    public ResponseEntity<String> sendUserInfoToSuperStream(@RequestBody UserDetailsDTO userDetailsDTO) {
        rabbitMqStreamProducer.publishUser(userDetailsDTO);
        return ResponseEntity.ok("Message Sent");
    }

    @PostMapping("/stream/user/address")
    @Description("Stream Queue With Filter")
    public ResponseEntity<String> sendUserAddressInfoToStream(@RequestBody UserDetailsDTO userDetailsDTO) {
        rabbitMqStreamProducer.publishUserInfoWithAddress(userDetailsDTO);
        return ResponseEntity.ok("Message Sent");
    }

    @PostMapping("/user/address")
    @Description("Stream Queue With Filter")
    public ResponseEntity<String> sendUserAddressInfo(@RequestBody AddressDTO addressDTO) {
        rabbitMqStreamProducer.publishUserInfoWithAddress(addressDTO);
        return ResponseEntity.ok("Message Sent");
    }


    @PostMapping("/superstream/user")
    @Description("Stream Queue With Filter")
    public ResponseEntity<String> sendUserAddressInfoToStream(@RequestBody UserDetailsDTO userDetailsDTO, @RequestParam(value = "useKey", required = false, defaultValue = "false") Boolean useKey) {
        if(useKey){
            superStreamProducer.publishUserInfoWithKey(userDetailsDTO);
            return ResponseEntity.ok("Message Sent");
        }
        superStreamProducer.publishUserInfoWithHash(userDetailsDTO);
        return ResponseEntity.ok("Message Sent");
    }

    @PostMapping("/superstream/address")
    @Description("Stream Queue With Filter")
    public ResponseEntity<String> sendUserAddressInfo(@RequestBody AddressDTO addressDTO, @RequestParam(value = "useKey", required = false, defaultValue = "false") Boolean useKey) {
        if(useKey){
            superStreamProducer.publishUserAddressInfoWithKey(addressDTO);
            return ResponseEntity.ok("Message Sent");
        }
        superStreamProducer.publishAddressWithHash(addressDTO);
        return ResponseEntity.ok("Message Sent");
    }
}

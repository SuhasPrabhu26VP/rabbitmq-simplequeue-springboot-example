package com.message_broker.rabbitmq_consumer.dto;

public record UserDetailsDTO(Long id,String firstName,String lastName,String address,String tracker) {
}

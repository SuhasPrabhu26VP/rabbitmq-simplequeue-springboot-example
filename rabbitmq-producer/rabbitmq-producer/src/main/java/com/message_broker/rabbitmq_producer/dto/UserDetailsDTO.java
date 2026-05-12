package com.message_broker.rabbitmq_producer.dto;

import java.io.Serializable;

public record UserDetailsDTO(Long id, String firstName, String lastName, String address, String tracker) implements Serializable {
}

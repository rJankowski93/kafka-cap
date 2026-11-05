package com.org.rjankowski.kafka.creditcardmonitor;

import tools.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class CreditCardTrnSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Serialize error", e);
        }
    }
}

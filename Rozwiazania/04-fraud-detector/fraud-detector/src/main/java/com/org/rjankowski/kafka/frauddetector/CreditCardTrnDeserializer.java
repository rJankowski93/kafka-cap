package com.org.rjankowski.kafka.frauddetector;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;

public class CreditCardTrnDeserializer implements Deserializer<CreditCardTrn> {
    private final JsonMapper objectMapper = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    @Override
    public CreditCardTrn deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), CreditCardTrn.class);
        } catch (Exception e) {
            throw new RuntimeException("Deserialize error", e);
        }
    }
}

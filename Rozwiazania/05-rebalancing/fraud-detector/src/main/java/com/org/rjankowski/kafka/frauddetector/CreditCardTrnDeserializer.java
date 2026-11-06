package com.org.rjankowski.kafka.frauddetector;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class CreditCardTrnDeserializer implements Deserializer<CreditCardTrn> {

    @Override
    public CreditCardTrn deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper =  new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), CreditCardTrn.class);
        } catch (IOException e) {
            throw new RuntimeException("Serialize error");
        }
    }
}

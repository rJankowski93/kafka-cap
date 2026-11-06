package com.org.rjankowski.kafka.paymentagregator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class OnlinePaymentDeserializer<T> implements Deserializer<OnlinePayment> {
    @Override
    public OnlinePayment deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper =  new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            return objectMapper.readValue(new String(data, "UTF-8"), OnlinePayment.class);
        } catch (IOException e) {
            throw new RuntimeException("Serialize error");
        }
    }
}

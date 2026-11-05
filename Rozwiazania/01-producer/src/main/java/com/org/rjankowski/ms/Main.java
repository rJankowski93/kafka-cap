package com.org.rjankowski.ms;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Producer<String, String> producer = createProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>("test",3,"key1","value10");
        RecordMetadata recordMetadata = producer.send(record).get();
        System.out.println(recordMetadata);
        producer.flush();
    }

    public static Producer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(properties);
    }
}

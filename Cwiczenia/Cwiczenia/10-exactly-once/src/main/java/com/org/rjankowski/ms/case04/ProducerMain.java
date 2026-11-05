package com.org.rjankowski.ms.case04;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerMain {

    private static String topic = "test-trn";
    private static String kafkaUrl = "localhost:9092";

    public static void main(String[] args) {
        Producer<String, String> producer = createProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key1", "value1");

        producer.send(record);
        producer.flush();
    }

    public static Producer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer");

        return new KafkaProducer<>(properties);
    }
}

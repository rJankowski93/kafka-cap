package com.org.rjankowski.kafka.onlinepaymentmonitor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;
import java.util.UUID;

@Configuration
public class Config {

    @Value("${kafka.url}")
    private String kafkaUrl;

    @Bean
    public Producer<String, OnlinePayment> kafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OnlinePaymentSerializer.class);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "online-payment-monitor");
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
        KafkaProducer<String, OnlinePayment> kafkaProducer = new KafkaProducer<>(properties);
        kafkaProducer.initTransactions();
        return kafkaProducer;
    }
}

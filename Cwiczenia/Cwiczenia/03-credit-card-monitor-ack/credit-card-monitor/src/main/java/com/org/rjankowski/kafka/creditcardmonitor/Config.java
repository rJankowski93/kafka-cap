package com.org.rjankowski.kafka.creditcardmonitor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class Config {

    @Value("${kafka.url}")
    private String kafkaUrl;

    @Value("${kafka.credit-card-trn-topic}")
    private String creditCardTrnTopic;

    @Bean
    public Producer<String, String> kafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CreditCardTrnSerializer.class);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, creditCardTrnTopic);
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CreditCardTrnPartitioner.class);
        return new KafkaProducer<>(properties);
    }
}

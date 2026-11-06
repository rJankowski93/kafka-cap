package com.org.rjankowski.kafka.frauddetector;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;
import java.util.UUID;

@Configuration
public class Config {

    @Value("${kafka.url}")
    private String kafkaUrl;

    @Value("${kafka.credit-card-trn-topic}")
    private String creditCardTrnTopic;

    @Bean
    public Consumer<String, CreditCardTrn> kafkaConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CreditCardTrnDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "fraud-detector-group");
        properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "fraud-detector-" + UUID.randomUUID());

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "com.org.rjankowski.kafka.frauddetector.MyAssignor");

        return new KafkaConsumer<>(properties);
    }


}

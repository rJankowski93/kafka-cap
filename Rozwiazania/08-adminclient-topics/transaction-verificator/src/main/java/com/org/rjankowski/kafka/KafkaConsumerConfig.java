package com.org.rjankowski.kafka;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@EnableKafka
@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig {


    private final KafkaTopicsVerifier KafkaTopicsVerifier;

    @PostConstruct
    public void initTopics() throws ExecutionException, InterruptedException {
        KafkaTopicsVerifier.initTopics();
    }

    @Value("${kafka.url}")
    private String kafkaUrl;

    @Bean
    public ConsumerFactory<String, SimpleTrn> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "transaction-verificator");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new JacksonJsonDeserializer<>(SimpleTrn.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SimpleTrn> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, SimpleTrn> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setRecordFilterStrategy(record -> record.value().value < 100);
        return factory;
    }

}

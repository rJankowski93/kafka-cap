package com.org.rjankowski.kafka.paymentagregator;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

@Component
@RequiredArgsConstructor
public class KafkaListener {

    private static Consumer kafkaConsumer;

    @Value("${kafka.online-payment-topic}")
    private String onlinePaymentTopic;

    @Autowired
    public void setKafkaConsumer(Consumer kafkaConsumer) {
        KafkaListener.kafkaConsumer = kafkaConsumer;
    }

    @Async
    public void listen() {
        kafkaConsumer.subscribe(List.of(onlinePaymentTopic));
        while (true) {
            ConsumerRecords<String, OnlinePayment> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, OnlinePayment> record : records) {
                System.out.println("Record for key: " + record.key() + " in topic " + record.topic());
                OnlinePaymentAggregation onlinePaymentAggregation = DbInMemory.aggregations.getOrDefault(record.key(), new OnlinePaymentAggregation(0D, 0L));
                onlinePaymentAggregation.sumValue += record.value().value;
                onlinePaymentAggregation.quantity++;
                DbInMemory.aggregations.put(record.key(), onlinePaymentAggregation);
            }
            kafkaConsumer.commitSync();
        }
    }

}

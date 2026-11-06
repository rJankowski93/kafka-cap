package com.org.rjankowski.kafka.frauddetector;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;

@Component
@RequiredArgsConstructor
public class KafkaListener {

    private final Consumer<String, CreditCardTrn> kafkaConsumer;

    @Value("${kafka.credit-card-trn-topic}")
    private String creditCardTrnTopic;
    Long lastTimestamp = null;

    @Async
    public void listen() {
        kafkaConsumer.subscribe(Collections.singletonList(creditCardTrnTopic));

        while (true) {
            ConsumerRecords<String, CreditCardTrn> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, CreditCardTrn> record : records) {
                System.out.println("Get trn for client: " + record.value().clientId + " and company: " + record.value().companyName);
                if (record.value().value > 1000) {
                    System.out.println("Possible fraud detection: " + "value of trn grater than 1000");
                }
                if (record.value().city.length() < 3) {
                    System.out.println("Possible fraud detection: " + "number of characters in the city name less than 1000");

                }
                if (lastTimestamp != null && record.timestamp() - lastTimestamp < 5000) {
                    System.out.println("Possible fraud detection: " + "time beetwen trn less than 5s");
                }
                lastTimestamp = record.timestamp();
            }
            kafkaConsumer.commitSync();

        }
    }

}

package com.org.rjankowski.kafka.frauddetector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class FraudDetectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(FraudDetectorApplication.class, args);
    }

    private final KafkaListener kafkaListener;

    public FraudDetectorApplication(KafkaListener kafkaListener) {
        this.kafkaListener = kafkaListener;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startListener() {
        kafkaListener.listen();
    }
}

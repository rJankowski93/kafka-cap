package com.org.rjankowski.kafka.paymentagregator;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
@RequiredArgsConstructor
public class PaymentAggregatorApplication {

	public static void main(String[] args) {
		SpringApplication.run(PaymentAggregatorApplication.class, args);
	}

	private final KafkaListener kafkaListener;

	@EventListener(ApplicationReadyEvent.class)
	public void startListener() {
		kafkaListener.listen();
	}
}

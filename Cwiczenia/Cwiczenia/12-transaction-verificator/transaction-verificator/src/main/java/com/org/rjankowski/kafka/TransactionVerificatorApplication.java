package com.org.rjankowski.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

@SpringBootApplication
public class TransactionVerificatorApplication {

	public static void main(String[] args) {
		SpringApplication.run(TransactionVerificatorApplication.class, args);
	}

}

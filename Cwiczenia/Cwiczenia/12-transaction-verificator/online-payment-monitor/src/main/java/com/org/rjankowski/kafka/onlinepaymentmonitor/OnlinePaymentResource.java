package com.org.rjankowski.kafka.onlinepaymentmonitor;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController("/")
@RequiredArgsConstructor
public class OnlinePaymentResource {

    private final Producer<String, OnlinePayment> kafkaProducer;

    @Value("${kafka.online-payment-topic}")
    private String onlinePaymentTopic;
    @PostMapping("/online-payment")
    public ResponseEntity onlinePayment(@RequestBody OnlinePayment onlinePayment) {
        System.out.println("Process online payment for client: " + onlinePayment.clientId + " and value: " + onlinePayment.value);
        kafkaProducer.beginTransaction();
        kafkaProducer.send(new ProducerRecord<>(onlinePaymentTopic, onlinePayment.clientId.toString(), onlinePayment));
        kafkaProducer.commitTransaction();
        return new ResponseEntity<>(HttpStatus.OK);
    }
}

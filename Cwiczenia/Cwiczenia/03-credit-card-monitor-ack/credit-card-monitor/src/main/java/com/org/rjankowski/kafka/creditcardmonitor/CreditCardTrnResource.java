package com.org.rjankowski.kafka.creditcardmonitor;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController("/")
@RequiredArgsConstructor
public class CreditCardTrnResource {

    @Value("${kafka.credit-card-trn-topic}")
    private String creditCardTrnTopic;

    private final Producer kafkaProducer;

    @PostMapping("/credit-card-trn")
    public ResponseEntity creditCardTrn(@RequestBody CreditCardTrn creditCardTrn) {
        kafkaProducer.send(new ProducerRecord<>(creditCardTrnTopic, creditCardTrn.clientId.toString(), creditCardTrn));
        System.out.println("Process credit card trn for client: " + creditCardTrn.clientId + " and value: " + creditCardTrn.value);
        return new ResponseEntity<>(HttpStatus.OK);
    }
}

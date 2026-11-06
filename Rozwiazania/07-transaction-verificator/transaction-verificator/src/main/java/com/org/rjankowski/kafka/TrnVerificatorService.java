package com.org.rjankowski.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;


@Service
public class TrnVerificatorService {

    private final KafkaTemplate<String, SimpleTrn> kafkaTemplate;

    public TrnVerificatorService(KafkaTemplate<String, SimpleTrn> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Value("${kafka.trn-to-verification-topic}")
    private String trnTOVerificationTopic;

    @KafkaListener(topics = {"credit-card-trn", "online-payment"}, groupId = "transaction-verificator")
    public void listen(@Payload SimpleTrn trn, @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key) {
        System.out.println("Trn for clientId: " + trn.clientId + " and value: " + trn.value);
        kafkaTemplate.send(trnTOVerificationTopic, trn);
    }

}

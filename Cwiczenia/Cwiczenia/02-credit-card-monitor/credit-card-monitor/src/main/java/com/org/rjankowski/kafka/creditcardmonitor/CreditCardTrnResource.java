package com.org.rjankowski.kafka.creditcardmonitor;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController("/")
public class CreditCardTrnResource {
    @PostMapping("/credit-card-trn")
    public ResponseEntity creditCardTrn(@RequestBody CreditCardTrn creditCardTrn) {
        System.out.println("Process credit card trn for client: " + creditCardTrn.clientId + " and value: " + creditCardTrn.value);
        return new ResponseEntity<>(HttpStatus.OK);
    }
}

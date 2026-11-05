package com.org.rjankowski.kafka.onlinepaymentmonitor;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController("/")
public class OnlinePaymentResource {
    @PostMapping("/online-payment")
    public ResponseEntity onlinePayment(@RequestBody OnlinePayment onlinePayment) {
        System.out.println("Process online payment for client: " + onlinePayment.clientId + " and value: " + onlinePayment.value);
        return new ResponseEntity<>(HttpStatus.OK);
    }
}

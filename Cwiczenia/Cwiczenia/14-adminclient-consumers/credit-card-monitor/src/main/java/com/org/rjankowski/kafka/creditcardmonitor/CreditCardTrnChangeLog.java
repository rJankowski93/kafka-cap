package com.org.rjankowski.kafka.creditcardmonitor;

import lombok.*;



@Builder
public class CreditCardTrnChangeLog {

    Long clientId;
    Double value;
    String city;
    String companyName;
}


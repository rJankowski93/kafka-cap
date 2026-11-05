package com.org.rjankowski.kafka.creditcardmonitor;

import lombok.Data;

@Data
public class CreditCardTrn {
    Long clientId;
    Double value;
    String city;
    String companyName;
}

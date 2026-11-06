package com.org.rjankowski.kafka.paymentagregator;

import lombok.Data;

@Data
public class OnlinePayment {
    Long clientId;
    Double value;
    String type;
}

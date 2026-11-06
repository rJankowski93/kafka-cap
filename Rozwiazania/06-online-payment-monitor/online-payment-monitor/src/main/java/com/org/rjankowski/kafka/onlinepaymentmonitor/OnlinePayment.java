package com.org.rjankowski.kafka.onlinepaymentmonitor;

import lombok.Data;

@Data
public class OnlinePayment {
    Long clientId;
    Double value;
    String type;
}

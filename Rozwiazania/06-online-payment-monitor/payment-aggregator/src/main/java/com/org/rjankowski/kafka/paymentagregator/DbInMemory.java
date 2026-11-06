package com.org.rjankowski.kafka.paymentagregator;

import lombok.AllArgsConstructor;

import java.util.HashMap;
import java.util.Map;

public class DbInMemory {
    static Map<String, OnlinePaymentAggregation> aggregations = new HashMap<>();
}


@AllArgsConstructor
class OnlinePaymentAggregation {
    Double sumValue;
    Long quantity;
}
package com.org.rjankowski.ms;

import org.apache.kafka.common.serialization.Serdes;

public class TransactionSerde extends Serdes.WrapperSerde<Transaction> {
    public TransactionSerde() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(Transaction.class));
    }
}

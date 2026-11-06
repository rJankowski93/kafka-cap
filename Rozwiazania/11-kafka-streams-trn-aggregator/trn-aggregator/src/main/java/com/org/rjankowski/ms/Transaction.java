package com.org.rjankowski.ms;

import java.io.Serializable;

public class Transaction implements Serializable {
    private Long clientId;
    private Double value;


    public Transaction() {
    }

    public Transaction(Long clientId, Double value) {
        this.clientId = clientId;
        this.value = value;
    }

    public Long getClientId() {
        return clientId;
    }

    public void setClientId(Long clientId) {
        this.clientId = clientId;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}

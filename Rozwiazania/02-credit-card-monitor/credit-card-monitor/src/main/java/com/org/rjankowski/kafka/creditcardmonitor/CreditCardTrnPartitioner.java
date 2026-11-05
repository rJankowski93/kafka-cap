package com.org.rjankowski.kafka.creditcardmonitor;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;


public class CreditCardTrnPartitioner implements Partitioner {

    private final int MAIN_CLIENT_ID = 1;
    private final int MAIN_PARTITIONER_NUMBER = 3;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        byte[] serializedKey = new StringSerializer().serialize(topic, null, String.valueOf(key));
        if (((CreditCardTrn) value).clientId == MAIN_CLIENT_ID) {
            return MAIN_PARTITIONER_NUMBER;
        }
        return Utils.toPositive(Utils.murmur2(serializedKey)) % (cluster.partitionsForTopic(topic).size() - 1);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}

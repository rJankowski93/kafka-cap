package com.org.rjankowski.ms;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownMemberIdException;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Main {

    final static String CONSUMER_GROUP = "transaction-verificator";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        AdminClient adminClient = createAdminClient();


        Map<TopicPartition, OffsetAndMetadata> offsets =
                adminClient.listConsumerGroupOffsets(CONSUMER_GROUP).partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, OffsetAndMetadata> resetOffsets = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
            resetOffsets.put(e.getKey(), new OffsetAndMetadata(0));
        }

        try {
            adminClient.alterConsumerGroupOffsets(CONSUMER_GROUP, resetOffsets).all().get();
        } catch (ExecutionException e) {
            System.out.println("Error: " + e.getMessage());
            if (e.getCause() instanceof UnknownMemberIdException) {
                System.out.println("Consumer group is active");
            }
        }

    }

    private static AdminClient createAdminClient() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return AdminClient.create(properties);
    }
}

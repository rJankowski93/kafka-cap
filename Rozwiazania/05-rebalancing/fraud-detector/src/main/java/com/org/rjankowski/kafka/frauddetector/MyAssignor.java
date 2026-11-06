package com.org.rjankowski.kafka.frauddetector;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class MyAssignor implements ConsumerPartitionAssignor {

    private static final String TARGET_TOPIC = "credit-card-trn";

    @Override
    public GroupAssignment assign(Cluster cluster, GroupSubscription groupSubscription) {

        Map<String, List<TopicPartition>> assignments = new HashMap<>();
        Set<String> members = groupSubscription.groupSubscription().keySet();

        // Collect all partitions for the subscribed topics
        List<TopicPartition> allPartitions = new ArrayList<>();

        cluster.partitionsForTopic(TARGET_TOPIC).forEach(partitionInfo ->
                allPartitions.add(new TopicPartition(TARGET_TOPIC, partitionInfo.partition()))
        );

        // Prepare assignments
        Iterator<TopicPartition> iterator = allPartitions.iterator();
        int maxAssigment = 0;
        for (String member : members) {
            if (maxAssigment >= allPartitions.size()) {
                break;
            }
            List<TopicPartition> assigned = new ArrayList<>();
            for (int i = 0; i < 2 && iterator.hasNext(); i++) {
                assigned.add(iterator.next());
                maxAssigment++;
            }
            assignments.put(member, assigned);
        }

        // Convert to GroupAssignment
        Map<String, Assignment> finalAssignments = new HashMap<>();
        for (Map.Entry<String, List<TopicPartition>> entry : assignments.entrySet()) {
            finalAssignments.put(entry.getKey(), new Assignment(entry.getValue(), null));
        }

        return new GroupAssignment(finalAssignments);
    }

    @Override
    public String name() {
        return "MyAssignor";
    }
}

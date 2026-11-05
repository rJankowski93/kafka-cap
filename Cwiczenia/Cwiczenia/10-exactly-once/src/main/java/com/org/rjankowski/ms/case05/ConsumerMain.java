package com.org.rjankowski.ms.case05;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class ConsumerMain {

    private static String topicInput = "test-trn";
    private static String topicOutput = "test-trn-output";
    private static String kafkaUrl = "localhost:9092";
    private static String CONSUMER_GROUP_ID = "test-consumer";

    public static void main(String[] args) throws InterruptedException {
        Consumer<String, String> consumer = createConsumer();
        Producer<String, String> producer = createProducer();

        producer.initTransactions();

        while (true) {
            consumer.subscribe(Collections.singletonList(topicInput));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            producer.beginTransaction();

            for (ConsumerRecord<String, String> record : records) {
                processMessage(producer, record);
            }

            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionedRecords = records.records(partition);
                long offset = partitionedRecords.get(partitionedRecords.size() - 1)
                        .offset();

                offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
            }

            producer.sendOffsetsToTransaction(offsetsToCommit, new ConsumerGroupMetadata(CONSUMER_GROUP_ID));

            producer.commitTransaction();
        }
    }

    public static Consumer<String, String> createConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return new KafkaConsumer<>(properties);
    }

    public static Producer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, CONSUMER_GROUP_ID);

        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-case-05-b");

        return new KafkaProducer<>(properties);
    }

    private static void processMessage(Producer<String, String> producer, ConsumerRecord<String, String> record)  {
        System.out.println(record.toString());
        producer.send(new ProducerRecord<>(topicOutput, record.key(), record.value()));
    }
}

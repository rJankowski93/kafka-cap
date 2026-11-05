package com.org.rjankowski.ms;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Commit tylko przeprocesowanego offsetu - asynchronicznie, przekazując w requeście
 * mapę partycja -> offset (bardziej skomplikowane, ale pełna kontrola nad tym co commitujemy).
 */
public class ConsumerMain {
    public static void main(String[] args) throws InterruptedException {
        Consumer<String, String> consumer = createConsumer();
        consumer.subscribe(Collections.singletonList("test"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

            // budujemy mapę: partycja -> offset NASTĘPNEJ wiadomości do przetworzenia
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
            for (ConsumerRecord<String, String> record : records) {
                processMessage(record);
                offsetsToCommit.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1));
            }

            // asynchroniczny commit konkretnej mapy offsetów (nie blokuje pętli)
            if (!offsetsToCommit.isEmpty()) {
                consumer.commitAsync(offsetsToCommit, (offsets, exception) -> {
                    if (exception != null) {
                        System.out.println("Commit nieudany dla " + offsets + " : " + exception.getMessage());
                    } else {
                        System.out.println("Commit OK: " + offsets);
                    }
                });
            }
        }
    }

    public static Consumer<String, String> createConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "commit-map-partition");

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<>(properties);
    }

    private static void processMessage(ConsumerRecord<String, String> record) throws InterruptedException {
        Thread.sleep(100);
        System.out.println("Key:" + record.key() + " | " + "Value: " + record.value());
    }
}

package com.org.rjankowski.ms;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "trn-stats-app2");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        // Strumienie wejściowe
        KStream<String, String> streamA = builder
                .stream("input-A", Consumed.with(Serdes.String(), Serdes.String()))
                .peek((key, value) -> System.out.println("Stream A: " + key + " -> " + value));

        KStream<String, String> streamB = builder
                .stream("input-B", Consumed.with(Serdes.String(), Serdes.String()))
                .peek((key, value) -> System.out.println("Stream B: " + key + " -> " + value));

        // Filtrowanie strumieni
        KStream<String, String> filteredStreamA = streamA.
                filter((key, value) -> Long.parseLong(value) > 0)
                .peek((key, value) -> System.out.println("Filtered Stream A: " + key + " -> " + value));

        KStream<String, String> filteredStreamB = streamB.
                filter((key, value) -> Long.parseLong(value) > 0)
                .peek((key, value) -> System.out.println("Filtered Stream B: " + key + " -> " + value));

        // Join
        filteredStreamA.join(
                        filteredStreamB,
                        (value1, value2) -> String.valueOf(Long.parseLong(value1) + Long.parseLong(value2)),
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(30)),
                        StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
                )
                .peek((key, value) -> System.out.println("Joined Stream A+B: " + key + " -> " + value))
                .to("output-sum-per-id", Produced.with(Serdes.String(), Serdes.String()));

        //Merge
        KStream<String, String> mergedStreams = filteredStreamA
                .merge(filteredStreamB)
                .peek((key, value) -> System.out.println("Merged Stream A and B: " + key + " -> " + value));
        mergedStreams.to("output-merge", Produced.with(Serdes.String(), Serdes.String()));

        //Sum
        mergedStreams
                .mapValues(value -> Long.parseLong(value))
                .groupBy((key, value) -> "all", Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30)))
                .reduce(
                        Long::sum, // Funkcja sumująca
                        Materialized.with(Serdes.String(), Serdes.Long())
                )
                .toStream()
                .peek((key, value) -> System.out.println("Window: " + key + " -> Sum: " + value))
                .to("output-sum", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class, Duration.ofSeconds(30).toMillis()), Serdes.Long()));



        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }
}

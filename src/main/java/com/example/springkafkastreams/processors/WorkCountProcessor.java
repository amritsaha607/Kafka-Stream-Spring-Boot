package com.example.springkafkastreams.processors;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;

import static com.example.springkafkastreams.constants.KafkaConstants.*;

@Component
public class WorkCountProcessor {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    void buildPipeline(StreamsBuilder builder) {
        KStream<String, String> messageStream = builder
                .stream(INPUT_TOPIC, Consumed.with(STRING_SERDE, STRING_SERDE));
        KTable<String, Long> wordCounts = messageStream
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                .groupBy((key, word) -> word, Grouped.with(STRING_SERDE, STRING_SERDE))
                .count(Materialized.as(COUNTS));
        wordCounts.toStream().to(OUTPUT_TOPIC);
    }
}

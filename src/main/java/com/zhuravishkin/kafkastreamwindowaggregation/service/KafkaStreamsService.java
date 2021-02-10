package com.zhuravishkin.kafkastreamwindowaggregation.service;

import com.zhuravishkin.kafkastreamwindowaggregation.topology.KafkaStreamsTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class KafkaStreamsService {
    private final KafkaStreamsConfiguration kStreamsConfigs;
    private final StreamsBuilder streamsBuilder;
    private final KafkaStreamsTopology kafkaStreamsTopology;

    public KafkaStreamsService(KafkaStreamsConfiguration kStreamsConfigs,
                               StreamsBuilder streamsBuilder,
                               KafkaStreamsTopology kafkaStreamsTopology) {
        this.kStreamsConfigs = kStreamsConfigs;
        this.streamsBuilder = streamsBuilder;
        this.kafkaStreamsTopology = kafkaStreamsTopology;
    }

    @PostConstruct
    public void postConstructor() {
        kafkaStreamsTopology.kStream(
                streamsBuilder,
                "src-topic",
                "out-topic",
                "kstream-reduce-state-store",
                "ktable-suppress-state"
        );
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                kStreamsConfigs.asProperties()
        );
        kafkaStreams.start();
    }
}

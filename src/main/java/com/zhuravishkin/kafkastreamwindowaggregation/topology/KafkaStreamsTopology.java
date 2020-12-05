package com.zhuravishkin.kafkastreamwindowaggregation.topology;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhuravishkin.kafkastreamwindowaggregation.model.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Slf4j
@Component
public class KafkaStreamsTopology {
    private final ObjectMapper objectMapper;
    private final Serde<User> userSerde;

    public KafkaStreamsTopology(ObjectMapper objectMapper, Serde<User> userSerde) {
        this.objectMapper = objectMapper;
        this.userSerde = userSerde;
    }

    public Topology kStream(StreamsBuilder kStreamBuilder,
                            String inputTopicName,
                            String outputTopicName) {
        kStreamBuilder
                .stream(inputTopicName, Consumed.with(Serdes.String(), Serdes.String()))
//                .mapValues((readOnlyKey, value) -> value)
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMillis(30000)))
                .reduce((value1, value2) -> value2)
//                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .map((key, value) -> KeyValue.pair(key.key(), value))
                .mapValues(this::getUserFromString)
                .to(outputTopicName, Produced.with(Serdes.String(), userSerde));
        return kStreamBuilder.build();
    }

    public User getUserFromString(String userString) {
        User user = null;
        try {
            user = objectMapper.readValue(userString, User.class);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage(), e);
        }
        log.info("the message is processed");
        return user;
    }
}

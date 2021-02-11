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
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

import java.time.Duration;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

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
                            String outputTopicName,
                            String windowStoreName,
                            String suppressedStoreName) {
        kStreamBuilder
                .stream(inputTopicName, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(this::getUserFromString)
                .groupByKey(Grouped.with(Serdes.String(), userSerde))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)).grace(Duration.ZERO))
                .aggregate(User::new, (key, value, aggregate) -> (new User(value.getPhoneNumber(),
                        value.getFirstName(),
                        value.getSurName(),
                        value.getUri() + "," + aggregate.getUri(),
                        value.getEventTime())), Materialized.with(Serdes.String(), userSerde))
                .suppress(Suppressed.untilWindowCloses(unbounded()).withName(suppressedStoreName))
                .toStream()
                .map((key, value) -> KeyValue.pair(key.key(), value))
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

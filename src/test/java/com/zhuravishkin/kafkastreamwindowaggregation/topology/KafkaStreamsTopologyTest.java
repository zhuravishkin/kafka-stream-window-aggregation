package com.zhuravishkin.kafkastreamwindowaggregation.topology;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@ExtendWith(SpringExtension.class)
@SpringBootTest
class KafkaStreamsTopologyTest {
    @SpyBean
    KafkaStreamsTopology kafkaStreamsTopology;

    @SpyBean
    KafkaStreamsConfiguration kStreamsConfigs;

    @SpyBean
    StreamsBuilder streamsBuilder;

    String event;
    String inputTopicName;
    String outputTopicName;
    String windowStoreName;
    String suppressedStoreName;

    TopologyTestDriver topologyTestDriver;
    TestInputTopic<String, String> inputTopic;
    TestOutputTopic<String, String> outputTopic;

    @SneakyThrows
    public KafkaStreamsTopologyTest() {
        event = Files.readString(Paths.get("src/test/resources/event.json"));
        inputTopicName = "src";
        outputTopicName = "out";
        windowStoreName = "kstream-reduce";
        suppressedStoreName = "ktable-suppress";
    }

    @BeforeEach
    void setUp() {
        topologyTestDriver = new TopologyTestDriver(
                kafkaStreamsTopology.kStream(streamsBuilder, inputTopicName, outputTopicName, windowStoreName, suppressedStoreName),
                kStreamsConfigs.asProperties()
        );
        inputTopic = topologyTestDriver.createInputTopic(
                inputTopicName,
                Serdes.String().serializer(),
                Serdes.String().serializer()
        );
        outputTopic = topologyTestDriver.createOutputTopic(
                outputTopicName,
                Serdes.String().deserializer(),
                Serdes.String().deserializer()
        );
    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    void topologyTest() {
        inputTopic.pipeInput("79336661111", event, LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli());
        inputTopic.pipeInput("79336661111", event, LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli());
        inputTopic.pipeInput("79336661111", event, LocalDateTime.now().plusSeconds(10).toInstant(ZoneOffset.UTC).toEpochMilli());
        log.warn(outputTopic.readValue());
        assertTrue(outputTopic.isEmpty());
    }
}
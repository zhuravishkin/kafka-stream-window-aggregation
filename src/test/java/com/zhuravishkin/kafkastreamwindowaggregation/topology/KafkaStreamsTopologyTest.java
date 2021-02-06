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

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@ExtendWith(SpringExtension.class)
@SpringBootTest
class KafkaStreamsTopologyTest {
    @SpyBean
    KafkaStreamsTopology kafkaStreamsTopology;

    @SpyBean
    KafkaStreamsConfiguration kafkaStreamsConfiguration;

    @SpyBean
    StreamsBuilder streamsBuilder;

    String event;
    String inputTopicName;
    String outputTopicName;
    TopologyTestDriver topologyTestDriver;
    TestInputTopic<String, String> inputTopic;
    TestOutputTopic<String, String> outputTopic;

    @SneakyThrows
    public KafkaStreamsTopologyTest() {
        event = Files.readString(Paths.get("src/test/resources/event.json"));
        inputTopicName = "src";
        outputTopicName = "out";
    }

    @BeforeEach
    void setUp() {
        topologyTestDriver = new TopologyTestDriver(
                kafkaStreamsTopology.kStream(streamsBuilder, inputTopicName, outputTopicName),
                kafkaStreamsConfiguration.asProperties()
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
        inputTopic.pipeInput(event);
        log.warn(outputTopic.readValue());
        assertTrue(outputTopic.isEmpty());
    }
}
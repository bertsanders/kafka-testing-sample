package com.walmart.demo.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.walmart.demo.kafka.model.CalculationInput;

@SpringBootTest
@EmbeddedKafka(topics = {"${sample.consumer-topic-name}", "${sample.producer-topic-name}"})
@TestPropertySource(properties = {"spring.kafka.bootstrap-servers=http://${spring.embedded.kafka.brokers}"})
class KafkaTestingSampleApplicationTests {

  private static final int NUMBER_OF_INPUTS = 10;
  private static final int MAX_NTH_PRIME = 1000;
  private static final int CONSUMER_TIMEOUT = 10;
  private static final int PRIME_CERTAINTY = 10;

  @Value("${sample.consumer-topic-name}")
  private String consumerTopic;

  @Autowired
  private EmbeddedKafkaBroker broker;
  @Autowired
  private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

  @Autowired
  private Producer testProducer;
  @Autowired
  private TestConsumer testConsumer;

  @BeforeEach
  void init() {
    waitForKafkaToStart();
  }

  private void waitForKafkaToStart() {
    kafkaListenerEndpointRegistry.getListenerContainers().forEach(messageListenerContainer ->
        ContainerTestUtils.waitForAssignment(messageListenerContainer, broker.getPartitionsPerTopic()));
  }

  @Test
  void contextLoads() {
  }

  @Test
  void testReceivingMessage() {
    CalculationInput input = new CalculationInput();
    input.setValues(Stream.generate(Math::random).limit(NUMBER_OF_INPUTS)
        .map(d -> (int) (d * MAX_NTH_PRIME))
        .collect(Collectors.toList()));

    testProducer.send(new ProducerRecord<>(consumerTopic, input));

    testConsumer.waitForRecord().orTimeout(CONSUMER_TIMEOUT, TimeUnit.SECONDS)
        .thenAcceptAsync(calculationOutput -> calculationOutput.getPrimes().forEach(prime -> {
          assertThat(prime).isNotNull();
          assertThat(prime.isProbablePrime(PRIME_CERTAINTY)).isTrue();
        }))
        .join();
  }
}

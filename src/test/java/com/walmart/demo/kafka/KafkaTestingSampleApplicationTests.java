package com.walmart.demo.kafka;

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
@EmbeddedKafka(topics = "${sample.topic-name}")
@TestPropertySource(properties = {"spring.kafka.bootstrap-servers=http://${spring.embedded.kafka.brokers}"})
class KafkaTestingSampleApplicationTests {

  @Value("${sample.topic-name}")
  private String topicName;

  @Autowired
  private EmbeddedKafkaBroker broker;
  @Autowired
  private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

  @Autowired
  private Producer testProducer;

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

    input.setValues(Stream.generate(Math::random).limit(10)
        .map(d -> (int) (d * 100))
        .collect(Collectors.toList()));

    testProducer.send(new ProducerRecord<>(topicName, input));

    try {
      Thread.sleep(8000); //TODO replace this bad sleep with something more deterministic
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}

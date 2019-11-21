package com.walmart.demo.kafka;

import java.util.Map;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.walmart.demo.kafka.model.CalculationInput;

@Configuration
public class KafkaTestConfig {

  @Bean
  public ProducerFactory testProducerFactory(EmbeddedKafkaBroker broker) {
    Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
    producerProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
    DefaultKafkaProducerFactory<String, CalculationInput> producerFactory = new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), new JsonSerializer<>());
    return producerFactory;
  }

  @Bean
  public Producer testProducer(ProducerFactory testProducerFactory) {
    return testProducerFactory.createProducer();
  }
}

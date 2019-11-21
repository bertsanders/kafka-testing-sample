package bertsanders.demo.kafka;

import java.util.Map;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import bertsanders.demo.kafka.model.CalculationInput;
import bertsanders.demo.kafka.model.CalculationOutput;

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

  @Bean
  public ConcurrentKafkaListenerContainerFactory<Object, Object> testConsumerContainerFactory(
      ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
      ConsumerFactory<Object, Object> testConsumerFactory) {
    ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory();
    configurer.configure(factory, testConsumerFactory);
    return factory;
  }

  @Bean
  public ConsumerFactory testConsumerFactory(EmbeddedKafkaBroker broker) {
    Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-consumer", "true", broker);
    consumerProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
    DefaultKafkaConsumerFactory<String, CalculationOutput> consumerFactory =
        new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new JsonDeserializer<>(CalculationOutput.class));
    return consumerFactory;
  }
}

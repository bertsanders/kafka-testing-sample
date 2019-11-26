package bertsanders.demo.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import bertsanders.demo.kafka.model.CalculationInput;
import bertsanders.demo.kafka.model.CalculationOutput;

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
  @Value("${sample.producer-topic-name}")
  private String producerTopic;

  @Autowired
  private EmbeddedKafkaBroker broker;
  @Autowired
  private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

  @Autowired
  private Producer<String, CalculationInput> testProducer;
  private Consumer<String, CalculationOutput> testConsumer;

  @BeforeEach
  void init() {
    waitForKafkaToStart();
    testConsumer = createSubscriber();
  }

  private void waitForKafkaToStart() {
    kafkaListenerEndpointRegistry.getListenerContainers().forEach(messageListenerContainer ->
        ContainerTestUtils.waitForAssignment(messageListenerContainer, broker.getPartitionsPerTopic()));
  }

  private Consumer<String, CalculationOutput> createSubscriber() {
    Consumer<String, CalculationOutput> consumer = testConsumerFactory.createConsumer();
    consumer.subscribe(Collections.singletonList(producerTopic));
    return consumer;
  }

  @AfterEach
  void cleanup() {
    testConsumer.close();
  }

  @Test
  void contextLoads() {
  }

  @Autowired
  @Qualifier("testConsumerFactory")
  private ConsumerFactory<String, CalculationOutput> testConsumerFactory;

  @Test
  void testReceivingMessage() {
    CalculationInput input = new CalculationInput();
    input.setValues(Stream.generate(Math::random).limit(NUMBER_OF_INPUTS)
        .map(d -> (int) (d * MAX_NTH_PRIME))
        .collect(Collectors.toList()));

    testProducer.send(new ProducerRecord<>(consumerTopic, input));

    ConsumerRecords<String, CalculationOutput> poll = testConsumer.poll(Duration.of(CONSUMER_TIMEOUT, ChronoUnit.SECONDS));

    Iterable<ConsumerRecord<String, CalculationOutput>> records = poll.records(producerTopic);
    assertThat(records).size().as("Producer sent no messages").isEqualTo(1);

    StreamSupport.stream(records.spliterator(), false)
        .flatMap(cr -> cr.value().getPrimes().stream())
        .forEach(prime -> {
          assertThat(prime).isNotNull();
          assertThat(prime.isProbablePrime(PRIME_CERTAINTY)).isTrue();
        });

  }
}

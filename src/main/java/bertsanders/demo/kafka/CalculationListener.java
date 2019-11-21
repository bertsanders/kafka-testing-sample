package bertsanders.demo.kafka;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import bertsanders.demo.kafka.model.CalculationInput;
import bertsanders.demo.kafka.model.CalculationOutput;

@Slf4j
@Component
@RequiredArgsConstructor
public class CalculationListener {

  private final ExpensiveService expensiveService;

  private final KafkaTemplate<String, CalculationOutput> kafkaTemplate;

  @Value("${sample.producer-topic-name}")
  private String producerTopic;


  @KafkaListener(topics = "${sample.consumer-topic-name}", autoStartup = "true")
  public void consume(CalculationInput input) {
    log.debug("Received inputs=" + input.getValues().toString());

    List<BigInteger> output = input.getValues().stream()
        .map(expensiveService::nthPrime)
        .collect(Collectors.toList());

    log.debug("Producing results=" + output.toString());

    kafkaTemplate.send(producerTopic, new CalculationOutput(output));
  }
}

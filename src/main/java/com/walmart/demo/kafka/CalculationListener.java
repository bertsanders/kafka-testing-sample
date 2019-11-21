package com.walmart.demo.kafka;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.walmart.demo.kafka.model.CalculationInput;
import com.walmart.demo.kafka.model.CalculationOutput;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
    List<BigInteger> output = input.getValues().stream()
        .map(expensiveService::nthPrime)
        .collect(Collectors.toList());

    log.debug("Producing results=" + output.toString());

    kafkaTemplate.send(producerTopic, new CalculationOutput(output));
  }
}

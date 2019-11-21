package com.walmart.demo.kafka;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.walmart.demo.kafka.model.CalculationInput;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class CalculationListener {

  private final ExpensiveService expensiveService;

  @KafkaListener(topics = "${sample.topic-name}", autoStartup = "true")
  public void consume(CalculationInput input) {
    List<BigInteger> output = input.getValues().stream()
        .map(expensiveService::nthPrime)
        .collect(Collectors.toList());
    log.info(output.toString());
  }
}

package com.walmart.demo.kafka;

import java.math.BigInteger;

import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class ExpensiveServiceTest {

  @Test
  void factorial() {
    ExpensiveService service = new ExpensiveService();
    for (int i = 0; i < 10; ++i) {
      int input = (int) (Math.random() * 2000.0);
      BigInteger factorial = service.nthPrime(input);
      log.info("{}, {}", input, factorial.toString());
    }
  }
}
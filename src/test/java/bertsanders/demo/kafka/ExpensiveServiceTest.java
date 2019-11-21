package bertsanders.demo.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;

import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class ExpensiveServiceTest {

  @Test
  void primes() {
    ExpensiveService service = new ExpensiveService();
    for (int i = 0; i < 10; ++i) {
      int input = (int) (Math.random() * 2000.0);
      BigInteger prime = service.nthPrime(input);
      assertThat(prime.isProbablePrime(10)).isTrue();
      log.debug("{}, {}", input, prime.toString());
    }
  }
}
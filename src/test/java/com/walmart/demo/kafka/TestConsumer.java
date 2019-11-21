package com.walmart.demo.kafka;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.walmart.demo.kafka.model.CalculationOutput;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@ConditionalOnBean(type = "org.springframework.kafka.test.EmbeddedKafkaBroker")
public class TestConsumer extends ConcurrentLinkedQueue<ConsumerRecord<String, CalculationOutput>> {

  @KafkaListener(id = "test-consumer",
      topics = "${sample.producer-topic-name}",
      autoStartup = "true",
      containerFactory = "testConsumerContainerFactory"
  )
  public void listenRetry(ConsumerRecord<String, CalculationOutput> consumerRecord) {
    log.debug("Received Result=" + consumerRecord.value());
    offer(consumerRecord);
  }

  public CompletableFuture<CalculationOutput> waitForRecord() {
    return CompletableFuture.supplyAsync(() -> {
      ConsumerRecord<String, CalculationOutput> record;
      while ((record = poll()) == null) {
        ;
      }
      return record.value();
    });
  }

}

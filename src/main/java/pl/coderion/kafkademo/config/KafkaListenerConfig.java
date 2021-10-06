package pl.coderion.kafkademo.config;

import java.util.Date;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import pl.coderion.kafkademo.model.Foo1;
import pl.coderion.kafkademo.model.Foo2;

@Slf4j
@Component
@KafkaListener(
    id = "multigroup",
    topics = {"topic1", "topic2"})
public class KafkaListenerConfig {

  @KafkaHandler
  public void foo1(Foo1 foo) {
    log.info("Received Foo1: " + foo.getMessage());
    if (foo.getMessage().startsWith("fail")) {
      throw new RuntimeException("failed");
    }
  }

  @KafkaHandler
  public void foo2(Foo2 foo) {
    log.info("Received Foo2: " + foo.getMessage());
    if (foo.getMessage().startsWith("fail")) {
      throw new RuntimeException("failed");
    }
  }

  @KafkaHandler
  public void date(Date d) {
    log.info("Received date: " + d.toString());
  }

  @KafkaHandler
  public void map(Map map) {
    log.info("Received map: " + map.toString());
  }
}

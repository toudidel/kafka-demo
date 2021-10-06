package pl.coderion.kafkademo.service;

import java.util.Date;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class Scheduler {

  @Autowired private KafkaTemplate<Object, Object> template;

  @Scheduled(cron = "*/5 * * * * *")
  public void schedule() {
    this.template.send("topic1", new Date());
    log.info("Sent date to topic1");
  }
}

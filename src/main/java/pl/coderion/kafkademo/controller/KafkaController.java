package pl.coderion.kafkademo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import pl.coderion.kafkademo.model.Foo1;
import pl.coderion.kafkademo.model.Foo2;

@RestController
public class KafkaController {

  @Autowired private KafkaTemplate<Object, Object> template;

  @GetMapping("/1/{message}")
  public String foo1(@PathVariable String message) {
    this.template.send("topic1", Foo1.builder().message(message).build());
    return message;
  }

  @GetMapping("/2/{message}")
  public String foo2(@PathVariable String message) {
    this.template.send("topic2", Foo2.builder().message(message).build());
    return message;
  }
}

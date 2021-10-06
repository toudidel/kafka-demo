package pl.coderion.kafkademo.config;

import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.ByteArrayJsonMessageConverter;
import org.springframework.kafka.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.converter.Jackson2JavaTypeMapper.TypePrecedence;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.util.backoff.FixedBackOff;
import pl.coderion.kafkademo.model.Foo1;
import pl.coderion.kafkademo.model.Foo2;

@Slf4j
@Configuration
public class KafkaConfig {

  @Bean
  public SeekToCurrentErrorHandler errorHandler(KafkaOperations<Object, Object> template) {
    return new SeekToCurrentErrorHandler(
        new DeadLetterPublishingRecoverer(template), new FixedBackOff(1000L, 1));
  }

  @KafkaListener(
      id = "dlt1Group",
      topics = {"topic1.DLT", "topic2.DLT"})
  public void dlt1Listen(String in) {
    log.info("Received from DLT: " + in);
  }

  @Bean
  public NewTopic topic1() {
    return new NewTopic("topic1", 1, (short) 1);
  }

  @Bean
  public NewTopic dlt1() {
    return new NewTopic("topic1.DLT", 1, (short) 1);
  }

  @Bean
  public NewTopic topic2() {
    return new NewTopic("topic2", 1, (short) 1);
  }

  @Bean
  public NewTopic dlt2() {
    return new NewTopic("topic2.DLT", 1, (short) 1);
  }

  @Bean
  public RecordMessageConverter converter() {
    ByteArrayJsonMessageConverter converter = new ByteArrayJsonMessageConverter();
    DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
    typeMapper.setTypePrecedence(TypePrecedence.TYPE_ID);
    typeMapper.addTrustedPackages("pl.coderion.kafkademo.model");
    Map<String, Class<?>> mappings = new HashMap<>();
    mappings.put("foo1", Foo1.class);
    mappings.put("foo2", Foo2.class);
    typeMapper.setIdClassMapping(mappings);
    converter.setTypeMapper(typeMapper);
    return converter;
  }
}

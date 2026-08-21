package com.ashimCS.learnKafka.user_service.kafkaTemplateTest;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;


@Component
@Slf4j
@RequiredArgsConstructor
public class BeanTest {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @PostConstruct
    public void test() {
        log.info("testing ###########################################################################");
        System.out.println(kafkaTemplate);
    }
}


/*
Why didn't we create KafkaTemplate?
Because Spring Boot creates it automatically.
You have:
spring:
  kafka:
    bootstrap-servers: localhost:9092

And presumably your project has the Spring Kafka dependency.
Something like:

<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
</dependency>

Spring Boot sees:

Spring Kafka dependency
        +
Kafka configuration
        ↓
KafkaAutoConfiguration
        ↓
KafkaTemplate bean

So somewhere inside Spring Boot's auto-configuration machinery, a KafkaTemplate is created for you.
Conceptually:

@Bean
KafkaTemplate<String, String> kafkaTemplate(...) {
    return new KafkaTemplate<>(producerFactory);
}

You didn't write that code.
Spring Boot did.

 */
package com.ashimCS.learnKafka.notification_service.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@Service
@Slf4j
public class UserKafkaConsumer { //CONSUMER - this will consume from user kafka service

    @KafkaListener(
            topics = "${kafka.topic.name:user-random-topic}"
    )
    public void consumeFromUserRandomTopic(String message) {
        log.info("Received message from user random topic: {}", message);
        System.out.println(message);
    }

    @KafkaListener( topics = "${kafka.topic.name:user-random-topic}")
    public void consumeFromUserRandomTopic2(String message) {
        log.info("Received message from user random topic2: {}", message);
        System.out.println(message);
    }

    @KafkaListener(topics = "${kafka.topic.name:user-random-topic}")
    public void consumeFromUserRandomTopic3(String message) {
        log.info("Received message from user random topic3: {}", message);
        System.out.println(message);
    }
}

// @kafkaListener : Spring abstraction for consuming Kafka records

/*
@Service
@Slf4j
public class UserKafkaConsumer { // this will consume from user kafka service

    @KafkaListener(
            topics = "${kafka.topic.name:user-random-topic}",
            groupId = "notification-service-group"
    )
    public void consumeFromUserRandomTopic(String message) {
        log.info("Received message from user random topic: {}", message);
        System.out.println(message);
    }
}*/

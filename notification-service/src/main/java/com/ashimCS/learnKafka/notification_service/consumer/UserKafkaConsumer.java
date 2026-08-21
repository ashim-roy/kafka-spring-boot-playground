package com.ashimCS.learnKafka.notification_service.consumer;

import com.ashimCS.learnKafka.notification_service.event.UserCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@Service
@Slf4j
public class UserKafkaConsumer { //CONSUMER - this will consume from user kafka service

    @KafkaListener(topics = "user-created-topic")
    public void handleUserCreated(UserCreatedEvent userCreatedEvent) {
        log.info("Received UserCreatedEvent {}", userCreatedEvent);
    }
    // we had already deifned some listeners/clients in the notification service, so we can have multiple listeners for the same topic, and they will all receive the same message.
    // This is because they are in different consumer groups. If they were in the same consumer group, only one of them would receive the message.
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

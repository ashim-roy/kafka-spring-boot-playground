package com.ashimCS.learnKafka.user_service.controller;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.User;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequestMapping("/users")
@RequiredArgsConstructor
public class UserController { //Producer

    private final KafkaTemplate<String, String> kafkaTemplate; // kafkaTemplate: Spring abstraction used to publish messages

    @Value("${kafka.topic.name:user-random-topic}")
    private String KAFKA_RANDOM_TOPIC;

    @PostMapping("/{message}")
    public ResponseEntity<String> sendMessage(@PathVariable String message){

      //  kafkaTemplate.send(KAFKA_RANDOM_TOPIC, message);
       // return ResponseEntity.ok("message queued");

        // sending key and message
        for (int i=0; i<1000; i++){
            kafkaTemplate.send(
                    KAFKA_RANDOM_TOPIC,
                    ""+i%2,
                    message + " - " + i
            );
        }
        return ResponseEntity.ok("message queued");

       /*
        //sending 1000s of messages
         for (int i=0; i<1000; i++){
             kafkaTemplate.send(KAFKA_RANDOM_TOPIC, message + " - " + i);
         }
         return ResponseEntity.ok(message);
       */

    }
}

// kafkaTemplate: Spring abstraction used to publish messages

/*
The UserController is acting as your Kafka Producer.

Its job here is to take an incoming HTTP request and publish messages/events to the Kafka topic.

Your flow
HTTP Client
    |
    | POST /users/hello
    ↓
UserController
    |
    | KafkaTemplate.send()
    ↓
Kafka Broker
    |
    ↓
user-random-topic
This line does the actual publishing
kafkaTemplate.send(KAFKA_RANDOM_TOPIC, message);

It means: Send this message to this Kafka topic.

For example:
kafkaTemplate.send("user-random-topic", "hello");

Kafka receives:

Topic: user-random-topic
Message: hello
Your current loop is testing high-volume publishing

You changed it to:

for (int i = 0; i < 1000; i++) {
    kafkaTemplate.send(
        KAFKA_RANDOM_TOPIC,
        message + " - " + i
    );
}

So if you call:

POST /users/hello
you're actually producing:

hello - 0
hello - 1
hello - 2
...
hello - 999

That's 1000 Kafka records.

So your architecture becomes:

             POST /users/hello
                    |
                    ▼
              UserController
                    |
              KafkaTemplate
                    |
          ┌─────────┴─────────┐
          │                   │
       Record 1            Record 2
       Record 3            Record 4
          ...                ...
          Record 1000
                    |
                    ▼
             Kafka Topic
          user-random-topic
             /    |    \
            P0   P1    P2

One important point
kafkaTemplate.send() is generally asynchronous.

So:

kafkaTemplate.send(...);

doesn't mean:

"Wait until Kafka has completely processed this message."
It means roughly:
"Give this record to the Kafka producer; it will handle sending it."
That's why you can rapidly call it 1000 times.  Your controller then immediately returns:
return ResponseEntity.ok(message);

So the HTTP response is not confirmation that all 1000 messages have been successfully persisted by Kafka.
For this exercise, that's actually useful because we're going to learn about producer batching, asynchronous sending, partitions, and throughput next.
 */
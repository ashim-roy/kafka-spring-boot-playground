package com.ashimCS.learnKafka.user_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topic.name:user-random-topic}")
    private String KAFKA_RANDOM_TOPIC;

    @Bean
    public NewTopic userRandomTopic() {
       // return new NewTopic("user-random-topic", 3, (short) 1); // Name, partition, replication factor
        return new NewTopic(KAFKA_RANDOM_TOPIC, 3, (short) 1);    // Name, partition, replication factor
    }
}

// The main job of this configuration is to tell Spring Kafka to create the Kafka topic for you
/*
and creates a NewTopic object containing:

Topic name        → user-random-topic
Partitions        → 3
Replication factor → 1

So conceptually:

Spring Boot starts
       ↓
KafkaTopicConfig
       ↓
NewTopic bean
       ↓
Spring Kafka checks Kafka
       ↓
Does user-random-topic exist?
       │
       ├── No → Create it
       │
       └── Yes → Don't recreate it

Your topic becomes:

user-random-topic
├── Partition 0
├── Partition 1
└── Partition 2
 */
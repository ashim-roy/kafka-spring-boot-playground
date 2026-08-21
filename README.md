

# Kafka Spring Boot Playground

A hands-on learning project for exploring **Apache Kafka with Spring Boot** and understanding event-driven communication between microservices.

The project currently contains a simple **User Service (Kafka Producer)** and **Notification Service (Kafka Consumer)** to demonstrate Kafka fundamentals such as topics, partitions, producers, consumers, consumer groups, message keys, and partition-based message distribution.

---

## Architecture

```text
                    HTTP Request
                         |
                         v
                +----------------+
                |   User Service |
                |   Port: 9050   |
                +-------+--------+
                        |
                        | KafkaTemplate
                        | Key + Message
                        v
                +----------------------+
                |      Apache Kafka    |
                |                      |
                |   user-random-topic  |
                |     /    |    \      |
                |    P0    P1    P2    |
                +----------+-----------+
                           |
                           | @KafkaListener
                           v
                +----------------------+
                | Notification Service |
                |      Port: 9060      |
                +----------------------+
````

---

## Services

### User Service

The User Service acts as a **Kafka Producer**.

It exposes a REST endpoint:

```http
POST /users/{message}
```

The service publishes messages to Kafka using Spring Kafka's `KafkaTemplate`.

Example:

```http
POST http://localhost:9050/users/hello
```

The service publishes messages to:

```text
user-random-topic
```

---

### Notification Service

The Notification Service acts as a **Kafka Consumer**.

It listens to:

```text
user-random-topic
```

using Spring Kafka's `@KafkaListener`.

```java
@KafkaListener(
    topics = "${kafka.topic.name:user-random-topic}",
    groupId = "notification-service-group"
)
public void consumeFromUserRandomTopic(String message) {
    log.info("Received message: {}", message);
}
```

---

## Technologies

* Java 21
* Spring Boot
* Spring Kafka
* Apache Kafka 4.3.1
* PostgreSQL
* Maven
* Kafbat Kafka UI
* IntelliJ IDEA

---

# Kafka Concepts Covered

## 1. Kafka Topics

The project uses:

```text
user-random-topic
```

Topics are used to organize and store Kafka events.

The User Service publishes messages to the topic and the Notification Service consumes them.

---

## 2. Kafka Partitions

The topic is configured with three partitions:

```java
@Bean
public NewTopic userRandomTopic() {
    return new NewTopic(
        KAFKA_RANDOM_TOPIC,
        3,
        (short) 1
    );
}
```

Result:

```text
user-random-topic
├── Partition 0
├── Partition 1
└── Partition 2
```

Partitions allow Kafka to distribute messages and support parallel consumption.

---

## 3. Kafka Producer

Spring Kafka's `KafkaTemplate` is used to publish messages.

```java
kafkaTemplate.send(
    KAFKA_RANDOM_TOPIC,
    message
);
```

---

## 4. Kafka Message Keys

The project also demonstrates sending messages with a key:

```java
kafkaTemplate.send(
    KAFKA_RANDOM_TOPIC,
    String.valueOf(i % 2),
    message + " - " + i
);
```

This produces messages with two keys:

```text
"0"
"1"
```

Kafka uses the key with its partitioning logic to determine the partition.

Conceptually:

```text
Key
 |
 v
Partitioner
 |
 v
Kafka Partition
```

Messages with the same key are consistently routed to the same partition, assuming the partitioning configuration remains unchanged.

This is important when ordering of related events needs to be maintained.

---

## 5. Consumer Groups

The Notification Service uses:

```text
notification-service-group
```

A consumer group allows multiple consumers to work together to consume partitions.

For example:

```text
Kafka Topic
├── P0 ──> Consumer 1
├── P1 ──> Consumer 2
└── P2 ──> Consumer 3
```

A partition is assigned to only one consumer within a consumer group at a time.

---

## 6. Multiple Consumers

The project also experiments with multiple `@KafkaListener` instances consuming from the same topic and consumer group.

This demonstrates how Kafka distributes partitions across consumers.

---

## 7. KafkaTemplate Auto-Configuration

The project demonstrates Spring Boot's Kafka auto-configuration.

Instead of manually creating a `KafkaTemplate` bean, Spring Boot automatically creates it when the required Kafka configuration and dependencies are available.

```java
private final KafkaTemplate<String, String> kafkaTemplate;
```

Configuration:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
```

---

## 8. Automatic Topic Creation with Spring Kafka

The project uses Spring Kafka's `NewTopic`:

```java
@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topic.name:user-random-topic}")
    private String KAFKA_RANDOM_TOPIC;

    @Bean
    public NewTopic userRandomTopic() {
        return new NewTopic(
            KAFKA_RANDOM_TOPIC,
            3,
            (short) 1
        );
    }
}
```

Spring Kafka uses this configuration to create the topic if it does not already exist.

---

# Local Kafka Setup

This project uses Apache Kafka running locally in **KRaft mode**.

Kafka was installed from the Apache Kafka distribution.

### Generate Cluster ID

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-run-class.sh kafka.tools.StorageTool random-uuid)"
```

### Format KRaft Storage

```bash
bin/kafka-run-class.sh kafka.tools.StorageTool format \
  --standalone \
  -t "$KAFKA_CLUSTER_ID" \
  -c config/server.properties
```

### Start Kafka

```bash
bin/kafka-run-class.sh kafka.Kafka config/server.properties
```

Kafka runs on:

```text
localhost:9092
```

> The KRaft storage formatting step is only required when initializing a new Kafka storage directory. It should not be performed every time Kafka is started.

---

# Kafka CLI Examples

## Create a Topic

```bash
bin/kafka-run-class.sh org.apache.kafka.tools.TopicCommand \
  --create \
  --topic quickstart-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

## Describe a Topic

```bash
bin/kafka-run-class.sh org.apache.kafka.tools.TopicCommand \
  --describe \
  --topic quickstart-events \
  --bootstrap-server localhost:9092
```

## Start a Producer

```bash
bin/kafka-run-class.sh org.apache.kafka.tools.ConsoleProducer \
  --topic quickstart-events \
  --bootstrap-server localhost:9092
```

Then enter messages:

```text
hello kafka
first event
second event
```

## Start a Consumer

```bash
bin/kafka-run-class.sh org.apache.kafka.tools.consumer.ConsoleConsumer \
  --topic quickstart-events \
  --from-beginning \
  --bootstrap-server localhost:9092
```

---

# Kafbat Kafka UI

[Kafbat Kafka UI](https://github.com/kafbat/kafka-ui) is used to visualize the local Kafka cluster.

The UI allows inspection of:

* Kafka clusters
* Topics
* Partitions
* Messages
* Consumer groups
* Offsets
* Message keys
* Message values

Local configuration:

```yaml
logging:
  level:
    root: INFO
    io.kafbat.ui: DEBUG

kafka:
  clusters:
    - name: local
      bootstrapServers: localhost:9092

auth:
  type: DISABLED
```

Kafbat UI runs locally on:

```text
http://localhost:8080
```

---

# Project Structure

```text
learn_kafka_project/
│
├── user-service/
│   ├── src/
│   │   └── main/
│   │       ├── java/
│   │       │   └── ...
│   │       └── resources/
│   │           ├── application.properties
│   │           └── application.yml
│   │
│   └── pom.xml
│
├── notification-service/
│   ├── src/
│   │   └── main/
│   │       ├── java/
│   │       │   └── ...
│   │       └── resources/
│   │           ├── application.properties
│   │           └── application.yml
│   │
│   └── pom.xml
│
└── README.md
```

---

# Running the Project

## 1. Start Kafka

```bash
bin/kafka-run-class.sh kafka.Kafka config/server.properties
```

## 2. Start User Service

Run the Spring Boot application:

```bash
cd user-service
./mvnw spring-boot:run
```

User Service:

```text
http://localhost:9050
```

## 3. Start Notification Service

```bash
cd notification-service
./mvnw spring-boot:run
```

Notification Service:

```text
http://localhost:9060
```

## 4. Send a message

```bash
curl -X POST http://localhost:9050/users/hello
```

The User Service publishes the message to Kafka.

The Notification Service consumes it:

```text
Received message from user random topic: hello
```

---

# Learning Roadmap

This repository is being built incrementally to explore Kafka and event-driven microservices.

Planned topics include:

* [x] Kafka installation and local setup
* [x] Kafka topics
* [x] Kafka partitions
* [x] Producers
* [x] Consumers
* [x] Consumer groups
* [x] Message keys
* [x] Key-based partitioning
* [x] Spring Kafka `KafkaTemplate`
* [x] Spring Kafka `@KafkaListener`
* [x] Automatic topic creation
* [x] Kafbat Kafka UI
* [ ] Kafka Schema Registry
* [ ] Avro serialization
* [ ] Producer/consumer configuration
* [ ] Consumer error handling
* [ ] Retry mechanisms
* [ ] Dead Letter Topics
* [ ] Idempotent consumers
* [ ] Kafka transactions
* [ ] Event-driven microservice communication
* [ ] Saga choreography
* [ ] Outbox Pattern
* [ ] Eventual consistency
* [ ] Kafka Streams

---

# Purpose

This repository is primarily a **hands-on Kafka learning playground**.

The goal is to understand not only how to configure Kafka with Spring Boot, but also how Kafka behaves internally with:

```text
Topics
   ↓
Partitions
   ↓
Keys
   ↓
Producers
   ↓
Consumers
   ↓
Consumer Groups
   ↓
Distributed Event-Driven Systems
```

More advanced Kafka concepts and production-oriented patterns will be added as the project evolves.

```




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



---

## Kafka Producer and Consumer Configuration

The project was extended to explore Kafka's producer and consumer configuration using Spring Boot.

### Producer Configuration

Spring Kafka provides `KafkaTemplate` as an abstraction for publishing messages to Kafka.

Example:

```java
@RestController
@Slf4j
@RequestMapping("/users")
@RequiredArgsConstructor
public class UserController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.name:user-random-topic}")
    private String KAFKA_RANDOM_TOPIC;

    @PostMapping("/{message}")
    public ResponseEntity<String> sendMessage(@PathVariable String message) {

        kafkaTemplate.send(KAFKA_RANDOM_TOPIC, message);

        return ResponseEntity.ok("message queued");
    }
}
```

The producer sends the message to the configured Kafka topic.

---

## Creating Kafka Topics with Spring Boot

A Kafka topic can be created automatically using a `NewTopic` bean.

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

The configuration means:

```text
Topic Name       → user-random-topic
Partitions       → 3
Replication      → 1
```

Spring Boot creates the topic when the application starts if it does not already exist.

---

## Kafka Message Keys and Partitioning

Kafka messages can contain:

- Key
- Value

Example:

```java
kafkaTemplate.send(
        KAFKA_RANDOM_TOPIC,
        "0",
        "hello kafka"
);
```

Here:

```text
Key   → "0"
Value → "hello kafka"
```

The key is important because Kafka uses the key to determine which partition should receive the message.

Conceptually:

```text
Message
   |
   | Key
   ↓
Hash(key)
   |
   ↓
Partition
```

Therefore, messages with the same key are normally routed to the same partition.

For example:

```text
user-101 → Partition 1
user-101 → Partition 1
user-101 → Partition 1

user-102 → Partition 2
user-102 → Partition 2
```

This is particularly important when message ordering needs to be maintained for a particular entity.

### Sending Messages with Keys

```java
for (int i = 0; i < 1000; i++) {
    kafkaTemplate.send(
            KAFKA_RANDOM_TOPIC,
            "" + i % 2,
            message + " - " + i
    );
}
```

This produces keys:

```text
0
1
0
1
0
1
...
```

The producer uses the key when determining the target partition.

---

## Sending Messages Without a Key

A message can also be sent without explicitly providing a key:

```java
kafkaTemplate.send(
        KAFKA_RANDOM_TOPIC,
        message
);
```

In this case, the producer does not have a key to use for key-based partitioning.

---

## Producer and Consumer Serialization

Kafka ultimately transports data as bytes.

A Java object cannot simply be sent directly over the network. The producer therefore serializes the key and value before sending them to Kafka.

```text
Java Object / String
        |
        ↓
   Serializer
        |
        ↓
      byte[]
        |
        ↓
      Kafka
```

The consumer performs the reverse operation:

```text
Kafka
  |
  ↓
byte[]
  |
  ↓
Deserializer
  |
  ↓
Java Object / String
```

### String Serialization

For String keys and values:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092

    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer

    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
```

### JSON Serialization

For Java objects, JSON can be used as the serialization format.

With Spring Boot 4 / Spring Kafka 4, the Jackson 3 compatible serializers are used:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092

    producer:
      key-serializer: org.apache.kafka.common.serialization.LongSerializer
      value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer

    consumer:
      key-deserializer: org.apache.kafka.common.serialization.LongDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JacksonJsonDeserializer
```

Conceptually:

```text
Producer

Java Object
    ↓
JacksonJsonSerializer
    ↓
JSON
    ↓
byte[]
    ↓
Kafka
```

Consumer:

```text
Kafka
  ↓
byte[]
  ↓
JacksonJsonDeserializer
  ↓
JSON
  ↓
Java Object
```

JSON is only one serialization format. Kafka applications can also use formats such as:

- String
- JSON
- Avro
- Protobuf

Kafka itself transports bytes; the producer and consumer decide how those bytes are serialized and deserialized.

---

## JSON Deserialization Configuration

When consuming JSON, the consumer needs to know which Java class should be created from the incoming JSON.

Example:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092

    consumer:
      key-deserializer: org.apache.kafka.common.serialization.LongDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JacksonJsonDeserializer

      properties:
        "[spring.json.value.default.type]": com.ashimCS.learnKafka.notification_service.event.UserCreatedEvent
        "[spring.json.trusted.packages]": com.ashimCS.learnKafka.notification_service.event
```

These two properties have different purposes:

```text
spring.json.value.default.type
→ What Java class should I create?

spring.json.trusted.packages
→ Am I allowed to create classes from this package?
```

For example:

```text
Kafka JSON
    ↓
JacksonJsonDeserializer
    ↓
UserCreatedEvent
    ↓
Check trusted package
    ↓
Create Java object
```

---

## Kafka Consumer

Messages can be consumed using Spring Kafka's `@KafkaListener`.

```java
@Service
@Slf4j
public class UserKafkaConsumer {

    @KafkaListener(
            topics = "${kafka.topic.name:user-random-topic}",
            groupId = "notification-service-group"
    )
    public void consumeFromUserRandomTopic(String message) {

        log.info(
                "Received message from user random topic: {}",
                message
        );
    }
}
```

The `@KafkaListener` tells Spring:

> Listen to the specified Kafka topic and invoke this method when a message is received.

The consumed message is injected into the method parameter:

```java
public void consumeFromUserRandomTopic(String message)
```

---

## Consumer Groups

A `groupId` identifies a Kafka consumer group.

For example:

```java
@KafkaListener(
        topics = "user-random-topic",
        groupId = "notification-service-group"
)
```

Consumers belonging to the same group share the partitions of a topic.

For a topic with three partitions:

```text
user-random-topic
       |
   3 partitions
   /    |    \
  P0    P1    P2
   \    |    /
    Consumer Group
```

If there are three consumers in the same group:

```text
Consumer 1 → Partition 0
Consumer 2 → Partition 1
Consumer 3 → Partition 2
```

This allows Kafka consumers to process messages in parallel.

A key concept is:

> **Within a consumer group, a partition is assigned to only one consumer at a time.**

---

## Producer → Kafka → Consumer Flow

The project demonstrates the complete event flow:

```text
                 User Service
                     |
                     |
              KafkaTemplate
                     |
                     ↓
                Serializer
                     |
                     ↓
                  Kafka
                     |
                     ↓
               Deserializer
                     |
                     ↓
            Notification Service
                     |
                     |
               @KafkaListener
                     |
                     ↓
              Event Processing
```

For JSON events:

```text
UserCreatedEvent
      ↓
JacksonJsonSerializer
      ↓
byte[]
      ↓
Kafka Topic
      ↓
byte[]
      ↓
JacksonJsonDeserializer
      ↓
UserCreatedEvent
```

---

## Kafka Producer Configuration

Some important producer configurations explored in the project:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092

    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all
      retries: 3
```

### `bootstrap-servers`

Specifies the Kafka broker address:

```text
localhost:9092
```

### `acks`

Controls the level of acknowledgement required from Kafka.

```yaml
acks: all
```

means the producer waits for acknowledgement from all in-sync replicas.

### `retries`

```yaml
retries: 3
```

Allows the producer to retry a failed send when the failure is retryable.

---

## Kafka Consumer Configuration

Example:

```yaml
spring:
  kafka:
    consumer:
      group-id: my-group
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest
      enable-auto-commit: false
      max-poll-records: 100
```

### `group-id`

Identifies the consumer group.

```yaml
group-id: my-group
```

Consumers with the same group ID share partitions.

### `auto-offset-reset`

```yaml
auto-offset-reset: earliest
```

When a consumer has no previously committed offset, it starts reading from the earliest available offset.

### `enable-auto-commit`

```yaml
enable-auto-commit: false
```

Disables automatic consumer offset commits, allowing the application/framework to control when offsets are committed.

Conceptually:

```text
Read Message
     ↓
Process Message
     ↓
Successful Processing
     ↓
Commit Offset
```

### `max-poll-records`

```yaml
max-poll-records: 100
```

Controls the maximum number of records returned in a single consumer `poll()` operation.

It does not mean the consumer can consume only 100 messages. It means each poll can return up to 100 records. The default value is 500. So, conceptually, a consumer can receive up to 500 records in a single poll() call. You can configure it to smaller or larger values depending on your application's processing capacity.

A more powerful server with sufficient CPU, memory, and processing capacity may be able to handle a higher number of records per poll. However, increasing this value does not automatically mean higher throughput—the application must also have enough resources to process the records efficiently.

---

## PostgreSQL and JPA

The User Service also persists users using Spring Data JPA and PostgreSQL.

Example entity:

```java
@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "app_users")
public class User {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String fullName;

    private String email;
}
```

Repository:

```java
public interface UserRepository
        extends JpaRepository<User, Long> {
}
```

The basic flow is:

```text
CreateUserRequestDto
        ↓
ModelMapper
        ↓
User Entity
        ↓
UserRepository.save()
        ↓
PostgreSQL
```

---

## User Created Event

An event object is used to represent information that can be published to Kafka.

Example:

```java
@Data
public class UserCreatedEvent {

    private Long id;

    private String email;
}
```

The event can be serialized into JSON before being published:

```text
UserCreatedEvent
      ↓
JacksonJsonSerializer
      ↓
JSON
      ↓
Kafka
```

The Notification Service can then deserialize the message:

```text
Kafka
  ↓
JacksonJsonDeserializer
  ↓
UserCreatedEvent
  ↓
@KafkaListener
```

---

## Key Kafka Concepts Covered

This project currently covers:

- Kafka brokers
- Topics
- Partitions
- Replication
- Producer
- Consumer
- Consumer groups
- Kafka keys
- Partition selection
- `KafkaTemplate`
- `@KafkaListener`
- Serialization
- Deserialization
- String serialization
- JSON serialization
- Jackson JSON serialization/deserialization
- Producer acknowledgements
- Producer retries
- Consumer offsets
- Auto offset reset
- Manual offset commit configuration
- Consumer polling
- PostgreSQL integration
- Spring Data JPA
- Event-based communication between services

---

## Current Architecture

```text
                  ┌────────────────────┐
                  │    User Service    │
                  │                    │
                  │ Spring Boot        │
                  │ PostgreSQL/JPA     │
                  │ KafkaTemplate      │
                  └─────────┬──────────┘
                            │
                            │ UserCreatedEvent
                            ↓
                    ┌───────────────┐
                    │     Kafka     │
                    │               │
                    │ user-random-  │
                    │ topic         │
                    │               │
                    │ 3 partitions  │
                    └───────┬───────┘
                            │
                            │ JSON event
                            ↓
                  ┌────────────────────┐
                  │ Notification       │
                  │ Service            │
                  │                    │
                  │ @KafkaListener     │
                  │ JSON Deserializer  │
                  └────────────────────┘
```

The project is intentionally being built incrementally to understand Kafka fundamentals first and then move toward more advanced event-driven microservice patterns.
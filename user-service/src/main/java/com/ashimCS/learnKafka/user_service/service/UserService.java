package com.ashimCS.learnKafka.user_service.service;


import com.ashimCS.learnKafka.user_service.dto.CreateUserRequestDto;
//import com.ashimCS.learnKafka.user_service.event.UserCreatedEvent;
import com.ashimCS.learnKafka.user_service.repository.UserRepository;
import com.ashimCS.learnKafka.user_service.entity.User;
import com.ashimCS.learnKafka.event.userCreatedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class UserService {

    @Value("${kafka.topic.user-created-topic}")
    private String KAFKA_USER_CREATED_TOPIC;  // injecting user_topic here

    private final ModelMapper modelMapper;
    private final UserRepository userRepository;
    private final KafkaTemplate<Long, userCreatedEvent> kafkaTemplate;

    public void createUser(CreateUserRequestDto createUserRequestDto) {

        // CREATE and SAVE user
        User user = modelMapper.map(createUserRequestDto, User.class);
        User savedUser = userRepository.save(user);

        // SENDING the event
        userCreatedEvent userCreatedEvent = modelMapper.map(savedUser, userCreatedEvent.class); //create userCreatdEvent object
        log.info("Event before Kafka: {}", userCreatedEvent);
        kafkaTemplate.send(KAFKA_USER_CREATED_TOPIC, userCreatedEvent.getId(), userCreatedEvent);  // topic name, key = eventID, value = event

    }
}

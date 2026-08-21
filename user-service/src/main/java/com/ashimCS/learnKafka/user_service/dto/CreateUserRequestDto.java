package com.ashimCS.learnKafka.user_service.dto;

import lombok.Data;

@Data
public class CreateUserRequestDto {
    private Long id;
    private String name;
    private String email;

}

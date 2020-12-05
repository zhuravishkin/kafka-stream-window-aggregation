package com.zhuravishkin.kafkastreamwindowaggregation.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class User {
    @JsonProperty("phone_number")
    private String phoneNumber;

    @JsonProperty("first_name")
    private String firstName;

    @JsonProperty("sur_name")
    private String surName;

    @JsonProperty("balance")
    private Integer balance;

    @JsonProperty("event_time")
    private Long eventTime;
}

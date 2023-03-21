package com.epam.task3.search.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.math.BigDecimal;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Employee {
    private String id;
    private String name;
    private Address address;
    private String email;
    private List<String> skills;
    private int experience;
    private double rating;
    private String description;
    private boolean verified;
    private BigDecimal salary;
    private String dob;
}

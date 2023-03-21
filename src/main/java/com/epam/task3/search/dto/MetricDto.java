package com.epam.task3.search.dto;

import lombok.Data;

import java.util.Map;

@Data
public class MetricDto {
    private String field;
    private Map<String, String> data;
}

package com.epam.task3.search.service;

import com.epam.task3.search.dto.Employee;
import com.epam.task3.search.dto.MetricDto;

import java.util.List;
import java.util.Optional;


public interface EmployeeService {
    List<Employee> findAll();

    Optional<Employee> findById(final String id);

    void create(final String id, final Employee employee);

    void delete(final String id);

    List<Employee> search(final String field, final String value, final String type);

    MetricDto aggregate(final String name, final String field, final String type);
}

package com.epam.task3.search.controller;

import com.epam.task3.search.dto.Employee;
import com.epam.task3.search.dto.MetricDto;
import com.epam.task3.search.exception.ResourceNotFoundException;
import com.epam.task3.search.service.EmployeeService;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;


@RestController
@RequestMapping("/employees")
public class EmployeeController {

    @Autowired
    @Qualifier("low-level-client")
    private EmployeeService employeeService;

    @Operation(summary = "Get all employees")
    @GetMapping
    public List<Employee> findAll() {
        return employeeService.findAll();
    }

    @Operation(summary = "Get an employee by id")
    @GetMapping("/{id}")
    public Employee findAById(final @PathVariable String id) {
        var result = employeeService.findById(id);

        if (result.isEmpty()) {
            throw new ResourceNotFoundException();
        }
        return result.get();
    }

    @Operation(summary = "Create an employee providing id and employee data json")
    @ResponseStatus(HttpStatus.CREATED)
    @PostMapping("/{id}")
    public void create(final @PathVariable String id, final @RequestBody Employee employee) {
        employeeService.create(id, employee);
    }

    @Operation(summary = "Delete an employee by its id")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    @DeleteMapping("/{id}")
    public void create(final @PathVariable String id) {
        employeeService.delete(id);
    }

    @Operation(summary = "Search employees by any field")
    @GetMapping("/search")
    public List<Employee> search(final @RequestParam String field, final @RequestParam String value,
                       final @RequestParam(defaultValue = "match") String type) {
        return employeeService.search(field, value, type);
    }

    @Operation(summary = "Get all employees with Java skill")
    @GetMapping("/java")
    public List<Employee> javaSearch() {
        return employeeService.search("skills", "Java", "match");
    }

    @Operation(summary = "Perform an aggregation by any numeric field with metric calculation")
    @GetMapping("/aggregate")
    public MetricDto aggregate(final @RequestParam String name, final @RequestParam String field,
                               final @RequestParam(defaultValue = "stats") String type) {
        return employeeService.aggregate(name, field, type);
    }
}

package com.epam.task3.search.service.impl;

import com.epam.task3.search.dto.Employee;
import com.epam.task3.search.dto.MetricDto;
import com.epam.task3.search.service.EmployeeService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;


@Slf4j
@Service("low-level-client")
@RequiredArgsConstructor
public class EmployeeServiceImpl implements EmployeeService {

    private static final String EMPLOYEES_DOC = "/employees/_doc/";
    private static final String EMPLOYEES_SEARCH = "/employees/_search";

    private final ObjectMapper mapper = new ObjectMapper();

    private final RestClient restClient;

    @SneakyThrows
    @Override
    public List<Employee> findAll() {
        var request = new Request("GET", EMPLOYEES_SEARCH);

        var response = restClient.performRequest(request);
        return parseEmployees(response);
    }

    @SneakyThrows
    @Override
    public void create(final String id, final Employee employee) {
        var request = new Request("POST", EMPLOYEES_DOC + id);
        request.setJsonEntity(mapper.writeValueAsString(employee));

        restClient.performRequest(request);
    }

    @SneakyThrows
    @Override
    public Optional<Employee> findById(final String id) {
        var request = new Request("GET", EMPLOYEES_DOC + id);

        var response = restClient.performRequest(request);
        var responseBody = EntityUtils.toString(response.getEntity());

        var json = mapper.readTree(responseBody);
        if (json.isEmpty()) {
            return Optional.empty();
        }

        var employee = mapper.treeToValue(json.get("_source"), Employee.class);
        employee.setId(id);

        return Optional.of(employee);
    }

    @SneakyThrows
    @Override
    public void delete(final String id) {
        var request = new Request("DELETE", EMPLOYEES_DOC + id);
        restClient.performRequest(request);
    }

    @SneakyThrows
    @Override
    public List<Employee> search(final String field, final String value, final String type) {
        var request = new Request("GET", EMPLOYEES_SEARCH);
        var json = mapper.createObjectNode();

        var queryNode = mapper.createObjectNode();
        var descNode = mapper.createObjectNode();

        if ("term".equals(type)) {
            var valueNode = mapper.createObjectNode();
            valueNode.put("value", value);
            descNode.set(field, valueNode);
        } else {
            descNode.put(field, value);
        }

        queryNode.set(type, descNode);
        json.set("query", queryNode);

        request.setJsonEntity(mapper.writeValueAsString(json));

        var response = restClient.performRequest(request);
        return parseEmployees(response);
    }

    @SneakyThrows
    @Override
    public MetricDto aggregate(String name, String field, String type) {
        var request = new Request("GET", EMPLOYEES_SEARCH);
        var json = mapper.createObjectNode();

        var queryNode = mapper.createObjectNode();
        var descNode = mapper.createObjectNode();
        var childNode = mapper.createObjectNode();

        descNode.put("field", field);

        queryNode.set(type, descNode);
        childNode.set(name, queryNode);
        json.set("aggs", childNode);

        request.setJsonEntity(mapper.writeValueAsString(json));

        var response = restClient.performRequest(request);

        return parseMetricDto(name, response);
    }

    @SneakyThrows
    private List<Employee> parseEmployees(final Response response) {
        var responseBody = EntityUtils.toString(response.getEntity());

        var json = mapper.readTree(responseBody);
        var elements = json.get("hits").get("hits");
        var data = elements.elements();

        var employees = new ArrayList<Employee>();
        while (data.hasNext()) {
            var curEmployee = data.next();
            var curId = curEmployee.get("_id");
            var employee = mapper.treeToValue(curEmployee.get("_source"), Employee.class);
            employee.setId(curId.asText());
            employees.add(employee);
        }

        return employees;
    }

    @SneakyThrows
    private MetricDto parseMetricDto(final String name, final Response response) {
        var responseBody = EntityUtils.toString(response.getEntity());

        var json = mapper.readTree(responseBody);
        var stats = json.get("aggregations").get(name);

        var metricDto = new MetricDto();
        metricDto.setField(name);

        var map = new HashMap<String, String>();

        var keys = stats.fieldNames();
        while (keys.hasNext()) {
            var key = keys.next();
            map.put(key, stats.get(key).asText());
        }

        metricDto.setData(map);
        return metricDto;
    }
}

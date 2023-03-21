package com.epam.task3.search.service.impl;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.util.ObjectBuilder;
import com.epam.task3.search.dto.Employee;
import com.epam.task3.search.dto.MetricDto;
import com.epam.task3.search.service.EmployeeService;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;


@Slf4j
@Service("java-api-client")
@RequiredArgsConstructor
public class DefaultEmployeeService implements EmployeeService {

    private static final String EMPLOYEES = "employees";

    private final ElasticsearchClient client;

    @SneakyThrows
    @Override
    public List<Employee> findAll() {
        var search = client.search(s -> s.index(EMPLOYEES), Employee.class);
        return parseEmployees(search);
    }

    @SneakyThrows
    @Override
    public void create(final String id, final Employee employee) {
        client.create(
                c -> c.index(EMPLOYEES)
                        .id(id)
                        .document(employee)
        );
    }

    @SneakyThrows
    @Override
    public Optional<Employee> findById(final String id) {
        var response = client.get(
                s -> s.index(EMPLOYEES).id(id), Employee.class
        );

        if (response.found() && response.source() != null) {
            var employee = response.source();
            employee.setId(response.id());
            return Optional.of(employee);
        }
        return Optional.empty();
    }

    @SneakyThrows
    @Override
    public void delete(final String id) {
        client.delete(d -> d.index(EMPLOYEES).id(id));
    }

    @SneakyThrows
    @Override
    public List<Employee> search(final String field, final String value, final String type) {
        final Function<Query.Builder, ObjectBuilder<Query>> query = "term".equals(type) ? (q -> q.term(
                t -> t.field(field)
                        .value(v -> v.stringValue(value))
        )) : (q -> q.match(m -> m.field(field).query(value)));

        var search = client.search(
                s -> s.index(EMPLOYEES).query(query), Employee.class
        );

        return parseEmployees(search);
    }

    @SneakyThrows
    @Override
    public MetricDto aggregate(String name, String field, String type) {
        var aggregation = switch (type) {
            case "min" -> Aggregation.of(a -> a.min(v -> v.field(field)));
            case "max" -> Aggregation.of(a -> a.max(v -> v.field(field)));
            case "sum" -> Aggregation.of(a -> a.sum(v -> v.field(field)));
            case "avg" -> Aggregation.of(a -> a.avg(v -> v.field(field)));
            default -> Aggregation.of(a -> a.stats(v -> v.field(field)));
        };

        var search = client.search(
                s -> s.index(EMPLOYEES).aggregations(name, aggregation), Employee.class
        );

        return parseMetricDto(name, search.aggregations());
    }

    @SneakyThrows
    private List<Employee> parseEmployees(final SearchResponse<Employee> search) {
        var employees = new ArrayList<Employee>();
        search.hits().hits().forEach(hit -> {
            var curEmployee = hit.source();
            if (curEmployee != null) {
                curEmployee.setId(hit.id());
            }
            employees.add(curEmployee);
        });
        return employees;
    }

    @SneakyThrows
    private MetricDto parseMetricDto(final String name, final Map<String, Aggregate> aggregateMap) {
        var metricDto = new MetricDto();
        metricDto.setField(name);

        var map = new HashMap<String, String>();
        aggregateMap.forEach((key, value) -> {
            if (value.isStats()) {
                var stats = value.stats();
                map.put("min", String.valueOf(stats.min()));
                map.put("max", String.valueOf(stats.max()));
                map.put("avg", String.valueOf(stats.avg()));
                map.put("sum", String.valueOf(stats.sum()));
            } else {
                map.put(key, value.simpleValue().valueAsString());
            }
        });


        metricDto.setData(map);
        return metricDto;
    }
}

package com.example.demo;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.EnableJdbcJobRepository;
import org.springframework.context.annotation.Configuration;

/**
 * Turns on Spring Batch with a JDBC-backed, persisting JobRepository (against the
 * "dataSource"/"transactionManager" beans from {@link DataSourceConfiguration}).
 *
 * <p>
 * This is the non-Boot equivalent of what the demo used to get from
 * spring-boot-starter-batch. Note that WITHOUT {@code @EnableJdbcJobRepository} the
 * default repository is the in-memory {@code ResourcelessJobRepository}, which persists
 * nothing — the same gotcha we hit under Spring Boot 4.
 */
@Configuration
@EnableBatchProcessing
@EnableJdbcJobRepository
public class BatchConfiguration {

}

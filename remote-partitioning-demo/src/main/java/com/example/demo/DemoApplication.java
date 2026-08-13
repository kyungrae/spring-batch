package com.example.demo;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.EnableJdbcJobRepository;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Single artifact, two roles. Run it twice:
 *   --spring.profiles.active=manager   (launches the job, splits + stops)
 *   --spring.profiles.active=worker    (executes partitions)
 *
 * Both point at the SAME datasource and the SAME RabbitMQ broker.
 */
@SpringBootApplication
@EnableBatchProcessing
@EnableJdbcJobRepository
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

}

package com.example.demo;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.support.JdbcTransactionManager;

/**
 * Hand-wired MySQL datasource + transaction manager (no Spring Boot autoconfig). The bean
 * NAMES matter: {@code @EnableJdbcJobRepository} looks up a DataSource named "dataSource"
 * and a PlatformTransactionManager named "transactionManager".
 *
 * <p>
 * Overridable with -Ddb.url / -Ddb.user / -Ddb.pass; defaults match the local MySQL used
 * in the demo (root, no password, database {@code spring_batch}).
 */
@Configuration
public class DataSourceConfiguration {

	@Bean
	public DataSource dataSource() {
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
		dataSource.setUrl(System.getProperty("db.url", "jdbc:mysql://localhost:3306/spring_batch"));
		dataSource.setUsername(System.getProperty("db.user", "root"));
		dataSource.setPassword(System.getProperty("db.pass", ""));
		return dataSource;
	}

	@Bean
	public JdbcTransactionManager transactionManager(DataSource dataSource) {
		return new JdbcTransactionManager(dataSource);
	}

	/**
	 * Replaces Boot's schema initialization. Runs the batch schema (drop + create) on the
	 * {@code local} and {@code manager} profiles — plus the PEOPLE business table + data
	 * on {@code local}. On {@code worker} it is disabled: the worker connects to the DB
	 * the manager already prepared and must NOT drop it.
	 */
	@Bean
	public DataSourceInitializer dataSourceInitializer(DataSource dataSource, Environment environment) {
		List<String> profiles = List.of(environment.getActiveProfiles());
		boolean prepareSchema = profiles.contains("local") || profiles.contains("manager");

		ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
		if (prepareSchema) {
			populator.addScript(new ClassPathResource("org/springframework/batch/core/schema-drop-mysql.sql"));
			populator.addScript(new ClassPathResource("org/springframework/batch/core/schema-mysql.sql"));
		}
		if (profiles.contains("local")) {
			populator.addScript(new ClassPathResource("app-schema-mysql.sql"));
			populator.addScript(new ClassPathResource("app-data-mysql.sql"));
		}

		DataSourceInitializer initializer = new DataSourceInitializer();
		initializer.setDataSource(dataSource);
		initializer.setEnabled(prepareSchema);
		initializer.setDatabasePopulator(populator);
		return initializer;
	}

}

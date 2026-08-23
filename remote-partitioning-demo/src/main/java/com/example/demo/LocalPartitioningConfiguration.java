package com.example.demo;

import javax.sql.DataSource;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.database.JdbcCursorItemReader;
import org.springframework.batch.infrastructure.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * LOCAL partitioning — single JVM, no broker — that splits WORK BY DATA RANGE.
 * <p>
 * The question this answers (Q1): "how does each partition decide which slice of the data
 * it is responsible for?" Here the {@link ColumnRangePartitioner} reads MIN/MAX(ID) of
 * the PEOPLE table and hands each partition a disjoint {@code [minValue, maxValue]} range
 * via its ExecutionContext. Each worker StepExecution then reads ONLY its own range
 * ({@code WHERE ID BETWEEN minValue AND maxValue}) and processes those rows.
 * <p>
 * PEOPLE has ids 1..10, gridSize=3 → ranges [1..4], [5..8], [9..10].
 * <p>
 * Notes on reading a partition safely:
 * <ul>
 * <li>the reader is {@code @StepScope}d, so every worker StepExecution gets its OWN
 * reader instance bound to its own range. That — not the reader flavour — is what makes
 * concurrent partitions safe.
 */
@Configuration
@Profile("local")
public class LocalPartitioningConfiguration {

	public record Person(long id, String name) {
	}

	/**
	 * Splits the PEOPLE table by the ID column into gridSize ranges. This is the "who
	 * owns which slice" step.
	 */
	@Bean
	public ColumnRangePartitioner peoplePartitioner(DataSource dataSource) {
		ColumnRangePartitioner partitioner = new ColumnRangePartitioner();
		partitioner.setDataSource(dataSource);
		partitioner.setTable("PEOPLE");
		partitioner.setColumn("ID");
		return partitioner;
	}

	/**
	 * Step-scoped reader: a fresh instance per worker StepExecution, bound to THAT
	 * partition's range. minValue/maxValue come from the partition's ExecutionContext
	 * (populated by the partitioner) and become bound SQL parameters.
	 */
	@Bean
	@StepScope
	public JdbcCursorItemReader<Person> peopleReader(DataSource dataSource,
			@Value("#{stepExecutionContext['minValue']}") Integer minValue,
			@Value("#{stepExecutionContext['maxValue']}") Integer maxValue) {
		return new JdbcCursorItemReaderBuilder<Person>().name("peopleReader")
			.dataSource(dataSource)
			.sql("SELECT ID, NAME FROM PEOPLE WHERE ID BETWEEN ? AND ? ORDER BY ID")
			.queryArguments(minValue, maxValue)
			.dataRowMapper(Person.class)
			.build();
	}

	/**
	 * Worker step = chunk step. Reads its assigned range and "processes" (prints) it. The
	 * thread name + the ids in each chunk make each partition's range visible at runtime.
	 */
	@Bean
	public Step localWorkerStep(JobRepository jobRepository, JdbcCursorItemReader<Person> peopleReader,
			PlatformTransactionManager transactionManager) {
		return new StepBuilder("workerStep", jobRepository).<Person, Person>chunk(5)
			.transactionManager(transactionManager)
			.reader(peopleReader)
			.writer(chunk -> System.out
				.println("[" + Thread.currentThread().getName() + "] processed range: " + chunk.getItems()))
			.build();
	}

	@Bean
	public Step localManagerStep(JobRepository jobRepository, ColumnRangePartitioner peoplePartitioner,
			Step localWorkerStep) {
		return new StepBuilder("managerStep", jobRepository).partitioner("workerStep", peoplePartitioner)
			.step(localWorkerStep)
			.gridSize(3)
			.taskExecutor(new SimpleAsyncTaskExecutor())
			.build();
	}

	@Bean
	public Job localPartitionedJob(JobRepository jobRepository, Step localManagerStep) {
		return new JobBuilder("localPartitionedJob", jobRepository).start(localManagerStep).build();
	}

}

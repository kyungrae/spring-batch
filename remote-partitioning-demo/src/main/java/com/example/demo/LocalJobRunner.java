package com.example.demo;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * LOCAL-only: launches the in-JVM partitioned job on startup. No broker, no worker
 * process needed — the partitions run on this JVM's thread pool.
 */
@Component
@Profile("local")
public class LocalJobRunner implements CommandLineRunner {

	private final JobOperator jobOperator;

	private final Job localPartitionedJob;

	public LocalJobRunner(JobOperator jobOperator, Job localPartitionedJob) {
		this.jobOperator = jobOperator;
		this.localPartitionedJob = localPartitionedJob;
	}

	@Override
	public void run(String... args) throws Exception {
		JobParameters params = new JobParametersBuilder().addLong("run.id", System.currentTimeMillis())
			.toJobParameters();
		JobExecution execution = jobOperator.start(localPartitionedJob, params);
		System.out.println("Launched localPartitionedJob, status = " + execution.getStatus() + ", id = "
				+ execution.getId());
	}

}

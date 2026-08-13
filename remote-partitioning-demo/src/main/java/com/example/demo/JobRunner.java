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
 * MANAGER-only: launches the job on startup, then (optionally) stops it mid-flight so you
 * can watch the manager JVM issue a stop while the partitions run on the worker JVM(s).
 *
 * The stop goes through JobOperator here on the manager; the worker step executions are
 * NOT in this JVM's AbstractStep lock map, so the stop update takes the null-lock branch.
 */
@Component
@Profile("manager")
public class JobRunner implements CommandLineRunner {

	private final JobOperator jobOperator;

	private final Job partitionedJob;

	public JobRunner(JobOperator jobOperator, Job partitionedJob) {
		this.jobOperator = jobOperator;
		this.partitionedJob = partitionedJob;
	}

	@Override
	public void run(String... args) throws Exception {
		JobParameters params = new JobParametersBuilder().addLong("run.id", System.currentTimeMillis())
			.toJobParameters();
		JobExecution execution = jobOperator.start(partitionedJob, params);

		// Uncomment to observe the cross-JVM stop race described in PR #5448:
		// Thread.sleep(500);
		// jobOperator.stop(execution.getId());

		System.out.println("Launched partitionedJob, execution id = " + execution.getId());
	}

}

package com.example.demo;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.integration.dsl.context.IntegrationFlowContext;

/**
 * Plain-Java entry point (no Spring Boot). Pick a profile with the first program
 * argument:
 *
 * <pre>
 *   ./mvnw exec:java -Dexec.args=local     # in-JVM range partitioning, launches the job
 *   ./mvnw exec:java -Dexec.args=manager   # remote manager: splits + sends requests
 *   ./mvnw exec:java -Dexec.args=worker    # remote worker: listens and executes partitions
 * </pre>
 *
 * All config classes are registered; {@code @Profile} decides which beans actually load.
 */
public final class Main {

	public static void main(String[] args) throws Exception {
		String profile = (args.length > 0 && !args[0].isBlank()) ? args[0].trim() : "local";

		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.getEnvironment().setActiveProfiles(profile);
		context.register(DataSourceConfiguration.class, BatchConfiguration.class, AmqpConfiguration.class,
				LocalPartitioningConfiguration.class, ManagerConfiguration.class, WorkerConfiguration.class);
		context.refresh();
		startRemotePartitioningFlows(context, profile);

		switch (profile) {
			case "local" -> {
				runJob(context, "localPartitionedJob");
				context.close();
			}
			case "manager" -> {
				runJob(context, "partitionedJob");
				context.close();
			}
			case "worker" -> {
				System.out.println("Worker started; listening for StepExecutionRequests. Ctrl+C to stop.");
				context.registerShutdownHook();
				Thread.currentThread().join(); // stay alive to service partitions
			}
			default -> {
				context.close();
				throw new IllegalArgumentException("Unknown profile: " + profile + " (use local|manager|worker)");
			}
		}
	}

	private static void runJob(AnnotationConfigApplicationContext context, String jobName) throws Exception {
		JobOperator jobOperator = context.getBean(JobOperator.class);
		Job job = context.getBean(jobName, Job.class);
		JobParameters params = new JobParametersBuilder().addLong("run.id", System.currentTimeMillis())
			.toJobParameters();
		JobExecution execution = jobOperator.start(job, params);
		System.out.println("Job [" + jobName + "] finished with status " + execution.getStatus());
	}

	/**
	 * The remote partitioning step builders register their integration flows with
	 * {@code autoStartup(false)} (RemotePartitioningWorkerStepBuilder:248,
	 * RemotePartitioningManagerStepBuilder:204), so nothing subscribes the
	 * StepExecutionRequestHandler to the requests channel unless we start them here.
	 * Symptom when missing: the broker redelivers every request forever ("Dispatcher has
	 * no subscribers") and the manager blocks in receiveReplies() with no timeout.
	 */
	private static void startRemotePartitioningFlows(AnnotationConfigApplicationContext context, String profile) {
		if (profile.equals("local")) {
			return;
		}
		IntegrationFlowContext flowContext = context.getBean(IntegrationFlowContext.class);
		flowContext.getRegistry().forEach((id, registration) -> {
			System.out.println("Starting integration flow registration: " + id);
			registration.start();
		});
	}

}

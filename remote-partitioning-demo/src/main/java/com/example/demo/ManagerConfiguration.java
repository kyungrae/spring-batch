package com.example.demo;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.core.partition.support.SimplePartitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.messaging.MessageChannel;

/**
 * MANAGER side (runs the Job). Its step is a PartitionStep whose PartitionHandler is a
 * MessageChannelPartitionHandler: it splits the work into N StepExecutions, sends one
 * StepExecutionRequest per partition to the "requests" queue, then waits for replies.
 *
 * It never calls workerStep.execute() itself — the step body runs on a worker JVM. The
 * JobOperator (and therefore any stop()) lives here.
 */
@Configuration
@Profile("manager")
public class ManagerConfiguration {

	// outbound: manager -> workers (requests)
	@Bean
	public DirectChannel requests() {
		return new DirectChannel();
	}

	@Bean
	public IntegrationFlow outboundRequests(RabbitTemplate rabbitTemplate) {
		return IntegrationFlow.from(requests())
			.handle(Amqp.outboundAdapter(rabbitTemplate).routingKey("requests"))
			.get();
	}

	// inbound: workers -> manager (replies)
	@Bean
	public DirectChannel replies() {
		return new DirectChannel();
	}

	@Bean
	public IntegrationFlow inboundReplies(ConnectionFactory connectionFactory) {
		return IntegrationFlow.from(Amqp.inboundAdapter(connectionFactory, "replies")).channel(replies()).get();
	}

	// the partitioned (manager) step
	@Bean
	public Step managerStep(
			JobRepository jobRepository,
			@Qualifier("requests") MessageChannel requests,
			@Qualifier("replies") MessageChannel replies
	) {
		return new RemotePartitioningManagerStepBuilder("managerStep", jobRepository)
			.partitioner("workerStep", partitioner()) // NAME of the worker step + how to split
			.gridSize(3) // 3 partitions -> 3 StepExecutions
			.outputChannel(requests) // where StepExecutionRequests go out
			.inputChannel(replies) // where worker results come back
			.build();
	}

	@Bean
	public Partitioner partitioner() {
		// Real jobs split by id-range / file / grid key; SimplePartitioner just makes N contexts.
		return new SimplePartitioner();
	}

	@Bean
	public Job partitionedJob(JobRepository jobRepository, Step managerStep) {
		return new JobBuilder("partitionedJob", jobRepository).start(managerStep).build();
	}

}

package com.example.demo;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilder;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.messaging.MessageChannel;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * WORKER side (runs in a SEPARATE JVM from the manager, sharing the same DB).
 *
 * RemotePartitioningWorkerStepBuilder wires a StepExecutionRequestHandler behind the
 * "requests" channel. For each incoming StepExecutionRequest it:
 *   1. reloads the StepExecution from the shared JobRepository (by id),
 *   2. calls workerStep.execute(stepExecution)  <-- execute() + the lock map live HERE,
 *   3. sends the resulting StepExecution back on the "replies" channel.
 */
@Configuration
@Profile("worker")
public class WorkerConfiguration {

	// inbound: manager -> this worker (requests)
	@Bean
	public DirectChannel requests() {
		return new DirectChannel();
	}

	@Bean
	public IntegrationFlow inboundRequests(ConnectionFactory connectionFactory) {
		return IntegrationFlow.from(Amqp.inboundAdapter(connectionFactory, "requests")).channel(requests()).get();
	}

	// outbound: this worker -> manager (replies)
	@Bean
	public DirectChannel replies() {
		return new DirectChannel();
	}

	@Bean
	public IntegrationFlow outboundReplies(RabbitTemplate rabbitTemplate) {
		return IntegrationFlow.from(replies())
			.handle(Amqp.outboundAdapter(rabbitTemplate).routingKey("replies"))
			.get();
	}

	// the actual worker step — its execute() is what PR #5448 is about
	@Bean
	public Step workerStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
			@Qualifier("requests") MessageChannel requests, @Qualifier("replies") MessageChannel replies) {

		return new RemotePartitioningWorkerStepBuilder("workerStep", jobRepository).inputChannel(requests)
			.outputChannel(replies)
			.tasklet((contribution, chunkContext) -> {
				System.out.println("Processing partition on worker: "
						+ chunkContext.getStepContext().getStepExecution().getSummary());
				Thread.sleep(2000); // simulate work so a stop can race the chunk commit
				return RepeatStatus.FINISHED;
			}, transactionManager)
			.build();
	}

}

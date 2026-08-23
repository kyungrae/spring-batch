package com.example.demo;

import java.util.List;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Hand-wired RabbitMQ beans for the remote profiles (previously provided by Spring Boot's
 * AMQP autoconfiguration). Only loaded for {@code manager}/{@code worker}; the local
 * profile needs no broker.
 *
 * <p>
 * The {@link RabbitAdmin} declares the two queues the manager/worker Integration flows
 * use ({@code requests}, {@code replies}). Overridable with -Damqp.host / -Damqp.port.
 */
@Configuration
@Profile({ "manager", "worker" })
public class AmqpConfiguration {

	@Bean
	public CachingConnectionFactory connectionFactory() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory(
				System.getProperty("amqp.host", "localhost"), Integer.getInteger("amqp.port", 5672));
		connectionFactory.setUsername(System.getProperty("amqp.user", "guest"));
		connectionFactory.setPassword(System.getProperty("amqp.pass", "guest"));
		return connectionFactory;
	}

	/**
	 * spring-amqp 4.x refuses to deserialize a Java-serialized payload unless its class
	 * matches an allowed pattern (see
	 * {@code org.springframework.amqp.utils.SerializationUtils}). Without this the worker
	 * rejects every StepExecutionRequest before it ever reaches the requests channel, and
	 * the broker redelivers it forever (millions of redeliveries, no visible error).
	 */
	@Bean
	public MessageConverter messageConverter() {
		SimpleMessageConverter converter = new SimpleMessageConverter();
		converter.setAllowedListPatterns(List.of("org.springframework.batch.*", "java.*"));
		return converter;
	}

	@Bean
	public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, MessageConverter messageConverter) {
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setMessageConverter(messageConverter);
		return template;
	}

	@Bean
	public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
		return new RabbitAdmin(connectionFactory);
	}

	@Bean
	public Queue requestsQueue() {
		return new Queue("requests", false);
	}

	@Bean
	public Queue repliesQueue() {
		return new Queue("replies", false);
	}

}

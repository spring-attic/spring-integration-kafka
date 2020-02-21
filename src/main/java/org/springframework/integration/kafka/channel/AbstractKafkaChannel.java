/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.kafka.channel;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.core.log.LogAccessor;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Abstract MessageChannel backed by a Kafka topic.
 *
 * @author Gary Russell
 * @since 3.3
 *
 */
public abstract class AbstractKafkaChannel extends AbstractMessageChannel {

	protected final LogAccessor logger = new LogAccessor(super.logger); // NOSONAR final

	private final KafkaTemplate<?, ?> template;

	protected final String topic; // NOSONAR final

	/**
	 * Construct an instance with the provided paramters.
	 * @param template the template.
	 * @param topic the topic.
	 */
	public AbstractKafkaChannel(KafkaTemplate<?, ?> template, String topic) {
		this.template = template;
		this.topic = topic;
	}

	@Override
	protected boolean doSend(Message<?> message, long timeout) {
		try {
			this.template.send(MessageBuilder.fromMessage(message)
						.setHeader(KafkaHeaders.TOPIC, this.topic)
						.build())
					.get(timeout, TimeUnit.MILLISECONDS);
		}
		catch (@SuppressWarnings("unused") InterruptedException e) {
			Thread.currentThread().interrupt();
			this.logger.debug(() -> "Interrupted while waiting for send result for: " + message);
			return false;
		}
		catch (ExecutionException e) {
			this.logger.error(e.getCause(), () -> "Interrupted while waiting for send result for: " + message);
			return false;
		}
		catch (TimeoutException e) {
			this.logger.debug(e, () -> "Timed out while waiting for send result for: " + message);
			return false;
		}
		return true;
	}

}

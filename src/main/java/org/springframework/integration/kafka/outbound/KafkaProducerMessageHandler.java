/*
 * Copyright 2013-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.kafka.outbound;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.integration.MessageTimeoutException;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

/**
 * Kafka Message Handler.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Soby Chacko
 * @author Artem Bilan
 * @author Gary Russell
 * @author Marius Bogoevici
 * @author Biju Kunjummen
 *
 * @since 0.5
 */
public class KafkaProducerMessageHandler<K, V> extends AbstractReplyProducingMessageHandler {

	private static final long DEFAULT_TIMEOUT = 10000;

	private final KafkaTemplate<K, V> kafkaTemplate;

	private final AbstractMessageListenerContainer<K, V> replyContainer;

	private final Map<String, SettableListenableFuture<ConsumerRecord<K, V>>> pendingReplies =
			new ConcurrentHashMap<>();

	private EvaluationContext evaluationContext;

	private Expression topicExpression;

	private Expression messageKeyExpression;

	private Expression partitionIdExpression;

	private volatile Expression timestampExpression;

	private boolean sync;

	private Expression sendTimeoutExpression = new ValueExpression<>(DEFAULT_TIMEOUT);

	private final long receiveTimeout = DEFAULT_TIMEOUT;

	/**
	 * Create a send-only message handler.
	 * @param kafkaTemplate the template.
	 */
	public KafkaProducerMessageHandler(final KafkaTemplate<K, V> kafkaTemplate) {
		Assert.notNull(kafkaTemplate, "kafkaTemplate cannot be null");
		this.kafkaTemplate = kafkaTemplate;
		this.replyContainer = null;
	}

	/**
	 * Create a send-and-receive message handler (outbound gateway).
	 * @param kafkaTemplate the template.
	 * @param replyContainer the reply container.
	 */
	public KafkaProducerMessageHandler(final KafkaTemplate<K, V> kafkaTemplate,
			AbstractMessageListenerContainer<K, V> replyContainer) {
		Assert.notNull(kafkaTemplate, "kafkaTemplate cannot be null");
		this.kafkaTemplate = kafkaTemplate;
		this.replyContainer = replyContainer;
		replyContainer.getContainerProperties().setMessageListener((MessageListener<K, V>) record -> {
			SettableListenableFuture<ConsumerRecord<K, V>> future =
					KafkaProducerMessageHandler.this.pendingReplies.get(record.key());
			if (future == null) {
				if (logger.isWarnEnabled()) {
					logger.warn("No pending request for " + record.toString());
				}
			}
			else {
				future.set(record);
			}
		});
	}

	public void setTopicExpression(Expression topicExpression) {
		this.topicExpression = topicExpression;
	}

	public void setMessageKeyExpression(Expression messageKeyExpression) {
		this.messageKeyExpression = messageKeyExpression;
	}

	public void setPartitionIdExpression(Expression partitionIdExpression) {
		this.partitionIdExpression = partitionIdExpression;
	}

	/**
	 * Specify a SpEL expression to evaluate a timestamp that will be added in the Kafka record.
	 * The resulting value should be a {@link Long} type representing epoch time in milliseconds.
	 *
	 * @param timestampExpression the {@link Expression} for timestamp to wait for result
	 * fo send operation.
	 * @since 3.0.0
	 */
	public void setTimestampExpression(Expression timestampExpression) {
		this.timestampExpression = timestampExpression;
	}

	public KafkaTemplate<?, ?> getKafkaTemplate() {
		return this.kafkaTemplate;
	}

	/**
	 * A {@code boolean} indicating if the {@link KafkaProducerMessageHandler}
	 * should wait for the send operation results or not. Defaults to {@code false}.
	 * In {@code sync} mode a downstream send operation exception will be re-thrown.
	 * @param sync the send mode; async by default.
	 * @since 2.0.1
	 */
	public void setSync(boolean sync) {
		this.sync = sync;
	}

	/**
	 * Specify a timeout in milliseconds for how long this
	 * {@link KafkaProducerMessageHandler} should wait wait for send operation
	 * results. Defaults to 10 seconds. The timeout is applied only in {@link #sync} mode.
	 * @param sendTimeout the timeout to wait for result fo send operation.
	 * @since 2.0.1
	 */
	@Override
	public void setSendTimeout(long sendTimeout) {
		setSendTimeoutExpression(new ValueExpression<>(sendTimeout));
	}

	/**
	 * Specify a SpEL expression to evaluate a timeout in milliseconds for how long this
	 * {@link KafkaProducerMessageHandler} should wait wait for send operation
	 * results. Defaults to 10 seconds. The timeout is applied only in {@link #sync} mode.
	 * @param sendTimeoutExpression the {@link Expression} for timeout to wait for result
	 * fo send operation.
	 * @since 2.1.1
	 */
	public void setSendTimeoutExpression(Expression sendTimeoutExpression) {
		Assert.notNull(sendTimeoutExpression, "'sendTimeoutExpression' must not be null");
		this.sendTimeoutExpression = sendTimeoutExpression;
	}

	@Override
	protected void doInit() {
		this.evaluationContext = ExpressionUtils.createStandardEvaluationContext(getBeanFactory());
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Object handleRequestMessage(final Message<?> requestMessage) {
		String topic = this.topicExpression != null ?
				this.topicExpression.getValue(this.evaluationContext, requestMessage, String.class)
				: requestMessage.getHeaders().get(KafkaHeaders.TOPIC, String.class);

		Assert.state(StringUtils.hasText(topic), "The 'topic' can not be empty or null");

		Integer partitionId = this.partitionIdExpression != null ?
				this.partitionIdExpression.getValue(this.evaluationContext, requestMessage, Integer.class)
				: requestMessage.getHeaders().get(KafkaHeaders.PARTITION_ID, Integer.class);

		Object messageKey = this.messageKeyExpression != null
				? this.messageKeyExpression.getValue(this.evaluationContext, requestMessage)
				: requestMessage.getHeaders().get(KafkaHeaders.MESSAGE_KEY);

		if (this.replyContainer != null) {
			if (messageKey != null) {
				if (logger.isDebugEnabled()) {
					logger.debug("Key '" + messageKey + "' ignored for request/reply");
				}
			}
			messageKey = UUID.randomUUID().toString();
			this.pendingReplies.put((String) messageKey, new SettableListenableFuture<>());
		}

		Long timestamp = this.timestampExpression != null
				? this.timestampExpression.getValue(this.evaluationContext, requestMessage, Long.class)
				: requestMessage.getHeaders().get(KafkaHeaders.TIMESTAMP, Long.class);

		V payload = (V) requestMessage.getPayload();
		if (payload instanceof KafkaNull) {
			payload = null;
		}

		ListenableFuture<?> future = this.kafkaTemplate.send(topic, partitionId, timestamp, (K) messageKey, payload);

		if (this.sync) {
			waitForAck(requestMessage, future);
		}
		if (this.replyContainer != null) {
			return obtainReply(requestMessage, messageKey);
		}
		else {
			return null;
		}
	}

	private void waitForAck(final Message<?> requestMessage, ListenableFuture<?> future) {
		Long sendTimeout = this.sendTimeoutExpression.getValue(this.evaluationContext, requestMessage, Long.class);
		try {
			if (sendTimeout == null || sendTimeout < 0) {
				future.get();
			}
			else {
				future.get(sendTimeout, TimeUnit.MILLISECONDS);
			}
		}
		catch (TimeoutException te) {
			throw new MessageTimeoutException(requestMessage, "Timeout waiting for response from KafkaProducer", te);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new MessageHandlingException(requestMessage, e);
		}
		catch (ExecutionException e) {
			throw new MessageHandlingException(requestMessage, e);
		}
	}

	private Object obtainReply(final Message<?> requestMessage, Object messageKey) {
		try {
			ConsumerRecord<K, V> reply = this.pendingReplies.get(messageKey).get(this.receiveTimeout,
					TimeUnit.MILLISECONDS);

			return getMessageBuilderFactory().withPayload(reply.value())
					.setHeader(KafkaHeaders.RECEIVED_TOPIC, reply.topic())
					.setHeader(KafkaHeaders.RECEIVED_PARTITION_ID, reply.partition());

		}
		catch (TimeoutException te) {
			throw new MessageTimeoutException(requestMessage, "Timeout waiting for response from peer", te);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new MessageHandlingException(requestMessage, e);
		}
		catch (ExecutionException e) {
			throw new MessageHandlingException(requestMessage, e);
		}
		finally {
			this.pendingReplies.remove(messageKey);
		}
	}

	@Override
	public String getComponentType() {
		return "kafka:outbound-channel-adapter";
	}

}

/*
 * Copyright 2013-2018 the original author or authors.
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

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.integration.MessageTimeoutException;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.integration.kafka.support.KafkaSendFailureException;
import org.springframework.integration.support.DefaultErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.kafka.support.JacksonPresent;
import org.springframework.kafka.support.KafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.SimpleKafkaHeaderMapper;
import org.springframework.kafka.support.converter.KafkaMessageHeaders;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.util.concurrent.SettableListenableFuture;

/**
 * Kafka Message Handler; when supplied with a {@link ReplyingKafkaTemplate}
 * it is used as the handler in an outbound gateway. When supplied with a simple
 * {@link KafkaTemplate} it used as the handler in an outbound channel adapter.
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

	private static final long DEFAULT_SEND_TIMEOUT = 10000;

	private final KafkaTemplate<K, V> kafkaTemplate;

	private final boolean isGateway;

	private final Map<String, Set<Integer>> replyTopicsAndPartitions = new HashMap<>();

	private EvaluationContext evaluationContext;

	private volatile Expression topicExpression;

	private volatile Expression messageKeyExpression;

	private volatile Expression partitionIdExpression;

	private volatile Expression timestampExpression;

	private boolean sync;

	private Expression sendTimeoutExpression = new ValueExpression<>(DEFAULT_SEND_TIMEOUT);

	private KafkaHeaderMapper headerMapper;

	private RecordMessageConverter replyMessageConverter = new MessagingMessageConverter();

	private MessageChannel sendFailureChannel;

	private String sendFailureChannelName;

	private MessageChannel sendSuccessChannel;

	private String sendSuccessChannelName;

	private ErrorMessageStrategy errorMessageStrategy = new DefaultErrorMessageStrategy();

	private Type replyPayloadType = Object.class;

	private volatile boolean noOutputChannel;

	public KafkaProducerMessageHandler(final KafkaTemplate<K, V> kafkaTemplate) {
		Assert.notNull(kafkaTemplate, "kafkaTemplate cannot be null");
		this.kafkaTemplate = kafkaTemplate;
		this.isGateway = kafkaTemplate instanceof ReplyingKafkaTemplate;
		if (this.isGateway) {
			setAsync(true);
			updateNotPropagatedHeaders(
					new String[] { KafkaHeaders.TOPIC, KafkaHeaders.PARTITION_ID, KafkaHeaders.MESSAGE_KEY }, false);
		}
		if (JacksonPresent.isJackson2Present()) {
			this.headerMapper = new DefaultKafkaHeaderMapper();
		}
		else {
			this.headerMapper = new SimpleKafkaHeaderMapper();
		}
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
	 * @param timestampExpression the {@link Expression} for timestamp to wait for result
	 * fo send operation.
	 * @since 2.3
	 */
	public void setTimestampExpression(Expression timestampExpression) {
		this.timestampExpression = timestampExpression;
	}

	/**
	 * Set the header mapper to use.
	 * @param headerMapper the mapper; can be null to disable header mapping.
	 * @since 2.3
	 */
	public void setHeaderMapper(KafkaHeaderMapper headerMapper) {
		this.headerMapper = headerMapper;
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
	 * Also applies when sending to the success or failure channels.
	 * @param sendTimeout the timeout to wait for result fo send operation.
	 * @since 2.0.1
	 */
	@Override
	public void setSendTimeout(long sendTimeout) {
		super.setSendTimeout(sendTimeout);
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

	/**
	 * Set the failure channel. After a send failure, an {@link ErrorMessage} will be sent
	 * to this channel with a payload of a {@link KafkaSendFailureException} with the
	 * failed message and cause.
	 * @param sendFailureChannel the failure channel.
	 * @since 2.1.2
	 */
	public void setSendFailureChannel(MessageChannel sendFailureChannel) {
		this.sendFailureChannel = sendFailureChannel;
	}

	/**
	 * Set the failure channel name. After a send failure, an {@link ErrorMessage} will be
	 * sent to this channel name with a payload of a {@link KafkaSendFailureException}
	 * with the failed message and cause.
	 * @param sendFailureChannelName the failure channel name.
	 * @since 2.1.2
	 */
	public void setSendFailureChannelName(String sendFailureChannelName) {
		this.sendFailureChannelName = sendFailureChannelName;
	}

	/**
	 * Set the success channel.
	 * @param sendSuccessChannel the Success channel.
	 * @since 3.0.2
	 */
	public void setSendSuccessChannel(MessageChannel sendSuccessChannel) {
		this.sendSuccessChannel = sendSuccessChannel;
	}

	/**
	 * Set the Success channel name.
	 * @param sendSuccessChannelName the Success channel name.
	 * @since 3.0.2
	 */
	public void setSendSuccessChannelName(String sendSuccessChannelName) {
		this.sendSuccessChannelName = sendSuccessChannelName;
	}

	/**
	 * Set the error message strategy implementation to use when sending error messages after
	 * send failures. Cannot be null.
	 * @param errorMessageStrategy the implementation.
	 * @since 2.1.2
	 */
	public void setErrorMessageStrategy(ErrorMessageStrategy errorMessageStrategy) {
		Assert.notNull(errorMessageStrategy, "'errorMessageStrategy' cannot be null");
		this.errorMessageStrategy = errorMessageStrategy;
	}

	/**
	 * Set a message converter for gateway replies.
	 * @param messageConverter the converter.
	 * @since 3.0.2
	 * @see #setReplyPayloadType(Class)
	 */
	public void setReplyMessageConverter(RecordMessageConverter messageConverter) {
		Assert.notNull(messageConverter, "'messageConverter' cannot be null");
		this.replyMessageConverter = messageConverter;
	}

	/**
	 * When using a type-aware message converter (such as {@code StringJsonMessageConverter},
	 * set the payload type the converter should create. Defaults to {@link Object}.
	 * @param payloadType the type.
	 * @since 3.0.2
	 * @see #setReplyMessageConverter(RecordMessageConverter)
	 */
	public void setReplyPayloadType(Type payloadType) {
		Assert.notNull(payloadType, "'payloadType' cannot be null");
		this.replyPayloadType = payloadType;
	}

	@Override
	public String getComponentType() {
		return this.isGateway ? "kafka:outbound-gateway" : "kafka:outbound-channel-adapter";
	}

	protected MessageChannel getSendFailureChannel() {
		if (this.sendFailureChannel != null) {
			return this.sendFailureChannel;
		}
		else if (this.sendFailureChannelName != null) {
			this.sendFailureChannel = getChannelResolver().resolveDestination(this.sendFailureChannelName);
			return this.sendFailureChannel;
		}
		return null;
	}

	protected MessageChannel getSendSuccessChannel() {
		if (this.sendSuccessChannel != null) {
			return this.sendSuccessChannel;
		}
		else if (this.sendSuccessChannelName != null) {
			this.sendSuccessChannel = getChannelResolver().resolveDestination(this.sendSuccessChannelName);
			return this.sendSuccessChannel;
		}
		return null;
	}

	@Override
	protected void doInit() {
		this.evaluationContext = ExpressionUtils.createStandardEvaluationContext(getBeanFactory());
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Object handleRequestMessage(final Message<?> message) {
		String topic = this.topicExpression != null ?
				this.topicExpression.getValue(this.evaluationContext, message, String.class)
				: message.getHeaders().get(KafkaHeaders.TOPIC, String.class);

		Assert.state(StringUtils.hasText(topic), "The 'topic' can not be empty or null");

		Integer partitionId = this.partitionIdExpression != null ?
				this.partitionIdExpression.getValue(this.evaluationContext, message, Integer.class)
				: message.getHeaders().get(KafkaHeaders.PARTITION_ID, Integer.class);

		Object messageKey = this.messageKeyExpression != null
				? this.messageKeyExpression.getValue(this.evaluationContext, message)
				: message.getHeaders().get(KafkaHeaders.MESSAGE_KEY);

		Long timestamp = this.timestampExpression != null
				? this.timestampExpression.getValue(this.evaluationContext, message, Long.class)
				: message.getHeaders().get(KafkaHeaders.TIMESTAMP, Long.class);

		V payload = (V) message.getPayload();
		if (payload instanceof KafkaNull) {
			payload = null;
		}

		Headers headers = null;
		if (this.headerMapper != null) {
			headers = new RecordHeaders();
			this.headerMapper.fromHeaders(message.getHeaders(), headers);
		}
		final ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, partitionId, timestamp,
				(K) messageKey, payload, headers);
		ListenableFuture<SendResult<K, V>> sendFuture;
		RequestReplyFuture<K, V, Object> gatewayFuture = null;
		MessageChannel metadataChannel = null;
		if (this.isGateway) {
			metadataChannel = getSendSuccessChannel();
			producerRecord.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, getReplyTopic(message)));
			gatewayFuture = ((ReplyingKafkaTemplate<K, V, Object>) this.kafkaTemplate).sendAndReceive(producerRecord);
			sendFuture = gatewayFuture.getSendFuture();
		}
		else {
			sendFuture = this.kafkaTemplate.send(producerRecord);
			// TODO: In 3.1, always use the success channel.
			if (!this.noOutputChannel) {
				metadataChannel = getOutputChannel();
				if (metadataChannel == null) {
					this.noOutputChannel = true;
				}
			}
			if (metadataChannel == null) {
				metadataChannel = getSendSuccessChannel();
			}
		}
		try {
			processSendResult(message, producerRecord, sendFuture, metadataChannel);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new MessageHandlingException(message, e);
		}
		catch (ExecutionException e) {
			// TODO: in 3.1 change this to e.getCause()
			throw new MessageHandlingException(message, e);
		}
		return processReplyFuture(gatewayFuture);
	}

	private byte[] getReplyTopic(final Message<?> message) {
		if (this.replyTopicsAndPartitions.isEmpty()) {
			determineValidReplyTopicsAndPartitions();
		}
		Object replyHeader = message.getHeaders().get(KafkaHeaders.REPLY_TOPIC);
		byte[] replyTopic = null;
		String topicToCheck = null;
		if (replyHeader instanceof String) {
			replyTopic = ((String) replyHeader).getBytes(StandardCharsets.UTF_8);
			topicToCheck = (String) replyHeader;
		}
		else if (replyHeader instanceof byte[]) {
			replyTopic = (byte[]) replyHeader;
		}
		else if (replyHeader != null) {
			throw new IllegalStateException(KafkaHeaders.REPLY_TOPIC + " must be String or byte[]");
		}
		if (replyTopic == null) {
			if (this.replyTopicsAndPartitions.size() == 1) {
				replyTopic = this.replyTopicsAndPartitions.keySet().iterator().next().getBytes(StandardCharsets.UTF_8);
			}
			else {
				throw new IllegalStateException("No reply topic header and no default reply topic is can be determined");
			}
		}
		else {
			if (topicToCheck == null) {
				topicToCheck = new String(replyTopic, StandardCharsets.UTF_8);
			}
			if (!this.replyTopicsAndPartitions.keySet().contains(topicToCheck)) {
				throw new IllegalStateException("The reply topic header ["
						+ topicToCheck +
						"] does not match any reply container topic: " + this.replyTopicsAndPartitions.keySet());
			}
		}
		Integer replyPartition = message.getHeaders().get(KafkaHeaders.REPLY_PARTITION, Integer.class);
		if (replyPartition != null) {
			if (topicToCheck == null) {
				topicToCheck = new String(replyTopic, StandardCharsets.UTF_8);
			}
			if (!this.replyTopicsAndPartitions.get(topicToCheck).contains(replyPartition)) {
				throw new IllegalStateException("The reply partition header ["
						+ replyPartition + "] does not match any reply container partition for topic ["
						+ topicToCheck + "]: " + this.replyTopicsAndPartitions.get(topicToCheck));
			}
		}
		return replyTopic;
	}

	private void determineValidReplyTopicsAndPartitions() {
		ReplyingKafkaTemplate<?, ?, ?> rkt = (ReplyingKafkaTemplate<?, ?, ?>) kafkaTemplate;
		Collection<TopicPartition> replyTopics = rkt.getAssignedReplyTopicPartitions();
		Map<String, Set<Integer>> topicsAndPartitions = new HashMap<>();
		if (replyTopics != null) {
			replyTopics.forEach(tp -> {
				topicsAndPartitions.computeIfAbsent(tp.topic(), (k) -> new TreeSet<>());
				topicsAndPartitions.get(tp.topic()).add(tp.partition());
			});
			this.replyTopicsAndPartitions.putAll(topicsAndPartitions);
		}
	}

	public void processSendResult(final Message<?> message, final ProducerRecord<K, V> producerRecord,
			ListenableFuture<SendResult<K, V>> future, MessageChannel metadataChannel)
			throws InterruptedException, ExecutionException {
		if (getSendFailureChannel() != null || metadataChannel != null) {
			future.addCallback(new ListenableFutureCallback<SendResult<K, V>>() {

				@Override
				public void onSuccess(SendResult<K, V> result) {
					if (metadataChannel != null) {
						KafkaProducerMessageHandler.this.messagingTemplate.send(metadataChannel,
								getMessageBuilderFactory().fromMessage(message)
										.setHeader(KafkaHeaders.RECORD_METADATA, result.getRecordMetadata()).build());
					}
				}

				@Override
				public void onFailure(Throwable ex) {
					if (getSendFailureChannel() != null) {
						KafkaProducerMessageHandler.this.messagingTemplate.send(getSendFailureChannel(),
								KafkaProducerMessageHandler.this.errorMessageStrategy.buildErrorMessage(
										new KafkaSendFailureException(message, producerRecord, ex), null));
					}
				}

			});
		}

		if (this.sync) {
			Long sendTimeout = this.sendTimeoutExpression.getValue(this.evaluationContext, message, Long.class);
			if (sendTimeout == null || sendTimeout < 0) {
				future.get();
			}
			else {
				try {
					future.get(sendTimeout, TimeUnit.MILLISECONDS);
				}
				catch (TimeoutException te) {
					throw new MessageTimeoutException(message, "Timeout waiting for response from KafkaProducer", te);
				}
			}
		}
	}

	private Future<?> processReplyFuture(RequestReplyFuture<?, ?, Object> future) {
		if (future == null) {
			return null;
		}
		return new ConvertingReplyFuture(future);
	}

	private final class ConvertingReplyFuture extends SettableListenableFuture<Object> {

		ConvertingReplyFuture(RequestReplyFuture<?, ?, Object> future) {
			addCallback(future);
		}

		private void addCallback(final RequestReplyFuture<?, ?, Object> future) {
			future.addCallback(new ListenableFutureCallback<ConsumerRecord<?, Object>>() {

				@Override
				public void onSuccess(ConsumerRecord<?, Object> result) {
					try {
						set(dontLeakHeaders(KafkaProducerMessageHandler.this.replyMessageConverter.toMessage(result,
								null, null, KafkaProducerMessageHandler.this.replyPayloadType)));
					}
					catch (Exception e) {
						setException(e);
					}
				}

				private Message<?> dontLeakHeaders(Message<?> message) {
					if (message.getHeaders() instanceof KafkaMessageHeaders) {
						Map<String, Object> headers = ((KafkaMessageHeaders) message.getHeaders()).getRawHeaders();
						headers.remove(KafkaHeaders.CORRELATION_ID);
						headers.remove(KafkaHeaders.REPLY_TOPIC);
						headers.remove(KafkaHeaders.REPLY_PARTITION);
						return message;
					}
					else {
						return getMessageBuilderFactory().fromMessage(message)
								.removeHeader(KafkaHeaders.CORRELATION_ID)
								.removeHeader(KafkaHeaders.REPLY_TOPIC)
								.removeHeader(KafkaHeaders.REPLY_PARTITION)
								.build();
					}
				}

				@Override
				public void onFailure(Throwable ex) {
					setException(ex);
				}

			});
		}

	}

}

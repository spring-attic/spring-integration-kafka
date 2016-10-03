/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.integration.kafka.inbound;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.integration.context.OrderlyShutdownCapable;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.adapter.BatchMessagingMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.FilteringAcknowledgingMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.FilteringBatchAcknowledgingMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.RetryingAcknowledgingMessageListenerAdapter;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.BatchMessageConverter;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

/**
 * Message-driven channel adapter.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Artem Bilan
 *
 */
public class KafkaMessageDrivenChannelAdapter<K, V> extends MessageProducerSupport implements OrderlyShutdownCapable {

	private final AbstractMessageListenerContainer<K, V> messageListenerContainer;

	private final RecordMessagingMessageListenerAdapter<K, V> recordListener = new IntegrationRecordMessageListener();

	private final BatchMessagingMessageListenerAdapter<K, V> batchListener = new IntegrationBatchMessageListener();

	private final ListenerMode mode;

	private RecordFilterStrategy<K, V> recordFilterStrategy;

	private boolean ackDiscarded;

	private RetryTemplate retryTemplate;

	private RecoveryCallback<Void> recoveryCallback;

	private boolean filterInRetry;

	/**
	 * Construct an instance with mode {@link ListenerMode#record}.
	 * @param messageListenerContainer the container.
	 */
	public KafkaMessageDrivenChannelAdapter(AbstractMessageListenerContainer<K, V> messageListenerContainer) {
		this(messageListenerContainer, ListenerMode.record);
	}

	/**
	 * Construct an instance with the provided mode.
	 * @param messageListenerContainer the container.
	 * @param mode the mode.
	 * @since 1.2
	 */
	public KafkaMessageDrivenChannelAdapter(AbstractMessageListenerContainer<K, V> messageListenerContainer,
			ListenerMode mode) {
		Assert.notNull(messageListenerContainer, "messageListenerContainer is required");
		Assert.isNull(messageListenerContainer.getContainerProperties().getMessageListener(),
				"Container must not already have a listener");
		this.messageListenerContainer = messageListenerContainer;
		this.messageListenerContainer.setAutoStartup(false);
		this.mode = mode;
	}

	/**
	 * Set the message converter; must be a {@link RecordMessageConverter} or
	 * {@link BatchMessageConverter} depending on mode.
	 * @param messageConverter the converter.
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		if (messageConverter instanceof RecordMessageConverter) {
			this.recordListener.setMessageConverter((RecordMessageConverter) messageConverter);
		}
		else if (messageConverter instanceof BatchMessageConverter) {
			this.batchListener.setBatchMessageConverter((BatchMessageConverter) messageConverter);
		}
		else {
			throw new IllegalArgumentException(
					"Message converter must be a 'RecordMessageConverter' or 'BatchMessageConverter'");
		}

	}

	/**
	 * Set the message converter to use with a record-based consumer.
	 * @param messageConverter the converter.
	 * @since 2.1
	 */
	public void setRecordMessageConverter(RecordMessageConverter messageConverter) {
		this.recordListener.setMessageConverter(messageConverter);
	}

	/**
	 * Set the message converter to use with a batch-based consumer.
	 * @param messageConverter the converter.
	 * @since 2.1
	 */
	public void setBatchMessageConverter(BatchMessageConverter messageConverter) {
		this.batchListener.setBatchMessageConverter(messageConverter);
	}

	/**
	 * Specify a {@link RecordFilterStrategy} to wrap
	 * {@link KafkaMessageDrivenChannelAdapter.IntegrationRecordMessageListener} into
	 * {@link FilteringAcknowledgingMessageListenerAdapter}.
	 * @param recordFilterStrategy the {@link RecordFilterStrategy} to use.
	 * @since 2.0.1
	 */
	public void setRecordFilterStrategy(RecordFilterStrategy<K, V> recordFilterStrategy) {
		this.recordFilterStrategy = recordFilterStrategy;
	}

	/**
	 * A {@code boolean} flag to indicate if {@link FilteringAcknowledgingMessageListenerAdapter}
	 * should acknowledge discarded records or not.
	 * Does not make sense if {@link #setRecordFilterStrategy(RecordFilterStrategy)} isn't specified.
	 * @param ackDiscarded true to ack (commit offset for) discarded messages.
	 * @since 2.0.1
	 */
	public void setAckDiscarded(boolean ackDiscarded) {
		this.ackDiscarded = ackDiscarded;
	}

	/**
	 * Specify a {@link RetryTemplate} instance to wrap
	 * {@link KafkaMessageDrivenChannelAdapter.IntegrationRecordMessageListener} into
	 * {@link RetryingAcknowledgingMessageListenerAdapter}.
	 * @param retryTemplate the {@link RetryTemplate} to use.
	 * @since 2.0.1
	 */
	public void setRetryTemplate(RetryTemplate retryTemplate) {
		Assert.isTrue(retryTemplate == null || this.mode.equals(ListenerMode.record),
				"Retry is not supported with mode=batch");
		this.retryTemplate = retryTemplate;
	}

	/**
	 * A {@link RecoveryCallback} instance for retry operation;
	 * if null, the exception will be thrown to the container after retries are exhausted.
	 * Does not make sense if {@link #setRetryTemplate(RetryTemplate)} isn't specified.
	 * @param recoveryCallback the recovery callback.
	 * @since 2.0.1
	 */
	public void setRecoveryCallback(RecoveryCallback<Void> recoveryCallback) {
		this.recoveryCallback = recoveryCallback;
	}

	/**
	 * The {@code boolean} flag to specify the order how
	 * {@link RetryingAcknowledgingMessageListenerAdapter} and
	 * {@link FilteringAcknowledgingMessageListenerAdapter} are wrapped to each other,
	 * if both of them are present.
	 * Does not make sense if only one of {@link RetryTemplate} or
	 * {@link RecordFilterStrategy} is present, or any.
	 * @param filterInRetry the order for {@link RetryingAcknowledgingMessageListenerAdapter} and
	 * {@link FilteringAcknowledgingMessageListenerAdapter} wrapping. Defaults to {@code false}.
	 * @since 2.0.1
	 */
	public void setFilterInRetry(boolean filterInRetry) {
		this.filterInRetry = filterInRetry;
	}

	/**
	 * When using a type-aware message converter (such as {@code StringJsonMessageConverter},
	 * set the payload type the converter should create. Defaults to {@link Object}.
	 * @param payloadType the type.
	 */
	public void setPayloadType(Class<?> payloadType) {
		this.recordListener.setFallbackType(payloadType);
		this.batchListener.setFallbackType(payloadType);
	}

	@Override
	protected void onInit() {
		super.onInit();

		if (this.mode.equals(ListenerMode.record)) {
			AcknowledgingMessageListener<K, V> listener = this.recordListener;

			boolean filterInRetry = this.filterInRetry && this.retryTemplate != null
					&& this.recordFilterStrategy != null;

			if (filterInRetry) {
				listener = new FilteringAcknowledgingMessageListenerAdapter<>(listener, this.recordFilterStrategy,
						this.ackDiscarded);
				listener = new RetryingAcknowledgingMessageListenerAdapter<>(listener, this.retryTemplate,
							this.recoveryCallback);
			}
			else {
				if (this.retryTemplate != null) {
					listener = new RetryingAcknowledgingMessageListenerAdapter<>(listener, this.retryTemplate,
							this.recoveryCallback);
				}
				if (this.recordFilterStrategy != null) {
					listener = new FilteringAcknowledgingMessageListenerAdapter<>(listener, this.recordFilterStrategy,
							this.ackDiscarded);
				}
			}
			this.messageListenerContainer.getContainerProperties().setMessageListener(listener);
		}
		else {
			BatchAcknowledgingMessageListener<K, V> listener = this.batchListener;

			if (this.recordFilterStrategy != null) {
				listener = new FilteringBatchAcknowledgingMessageListenerAdapter<>(listener, this.recordFilterStrategy,
						this.ackDiscarded);
			}
			this.messageListenerContainer.getContainerProperties().setMessageListener(listener);
		}
	}

	@Override
	protected void doStart() {
		this.messageListenerContainer.start();
	}

	@Override
	protected void doStop() {
		this.messageListenerContainer.stop();
	}

	@Override
	public String getComponentType() {
		return "kafka:message-driven-channel-adapter";
	}

	@Override
	public int beforeShutdown() {
		this.messageListenerContainer.stop();
		return getPhase();
	}

	@Override
	public int afterShutdown() {
		return getPhase();
	}

	/**
	 * The listener mode for the container, record or batch.
	 * @since 1.2
	 *
	 */
	public enum ListenerMode {

		/**
		 * Each {@link Message} will be converted from a single {@code ConsumerRecord}.
		 */
		record,

		/**
		 * Each {@link Message} will be converted from the {@code ConsumerRecords}
		 * returned by a poll.
		 */
		batch
	}

	private class IntegrationRecordMessageListener extends RecordMessagingMessageListenerAdapter<K, V> {

		IntegrationRecordMessageListener() {
			super(null, null);
		}

		@Override
		public void onMessage(ConsumerRecord<K, V> record, Acknowledgment acknowledgment) {
			Message<?> message = null;
			try {
				message = toMessagingMessage(record, acknowledgment);
			}
			catch (RuntimeException e) {
				Exception exception = new ConversionException("Failed to convert to message for: " + record, e);
				if (getErrorChannel() != null) {
					getMessagingTemplate().send(getErrorChannel(), new ErrorMessage(exception));
				}
			}
			if (message != null) {
				sendMessage(message);
			}
			else {
				KafkaMessageDrivenChannelAdapter.this.logger.debug("Converter returned a null message for: "
						+ record);
			}
		}

	}

	private class IntegrationBatchMessageListener extends BatchMessagingMessageListenerAdapter<K, V> {

		IntegrationBatchMessageListener() {
			super(null, null);
		}

		@Override
		public void onMessage(List<ConsumerRecord<K, V>> records, Acknowledgment acknowledgment) {
			Message<?> message = null;
			try {
				message = toMessagingMessage(records, acknowledgment);
			}
			catch (RuntimeException e) {
				Exception exception = new ConversionException("Failed to convert to message for: " + records, e);
				if (getErrorChannel() != null) {
					getMessagingTemplate().send(getErrorChannel(), new ErrorMessage(exception));
				}
			}
			if (message != null) {
				sendMessage(message);
			}
			else {
				KafkaMessageDrivenChannelAdapter.this.logger.debug("Converter returned a null message for: "
						+ records);
			}
		}

	}

}

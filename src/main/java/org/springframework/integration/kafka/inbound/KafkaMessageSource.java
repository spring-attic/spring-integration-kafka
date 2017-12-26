/*
 * Copyright 2018 the original author or authors.
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

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.endpoint.AbstractMessageSource;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.integration.support.AcknowledgmentCallback;
import org.springframework.integration.support.AcknowledgmentCallbackFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * Polled message source for kafka.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 3.0.1
 *
 */
public class KafkaMessageSource<K, V> extends AbstractMessageSource<Object>
		implements DisposableBean {

	private static final long DEFAULT_POLL_TIMEOUT = 50L;

	private final ConsumerFactory<K, V> consumerFactory;

	private final KafkaAckCallbackFactory<K, V> ackCallbackFactory;

	private final String[] topics;

	private String groupId;

	private String clientId = "message.source";

	private long pollTimeout = DEFAULT_POLL_TIMEOUT;

	private RecordMessageConverter messageConverter = new MessagingMessageConverter();

	private Type payloadType;

	private volatile Consumer<K, V> consumer;

	private volatile Iterator<ConsumerRecord<K, V>> recordIterator;

	private volatile Collection<TopicPartition> partitions;

	public KafkaMessageSource(ConsumerFactory<K, V> consumerFactory, String... topics) {
		this(consumerFactory, new KafkaAckCallbackFactory<K, V>(), topics);
	}

	public KafkaMessageSource(ConsumerFactory<K, V> consumerFactory,
			KafkaAckCallbackFactory<K, V> ackCallbackFactory, String... topics) {
		Assert.notNull(consumerFactory, "'consumerFactory' must not be null");
		Assert.notNull(ackCallbackFactory, "'ackCallbackFactory' must not be null");
		this.consumerFactory = consumerFactory;
		this.ackCallbackFactory = ackCallbackFactory;
		this.topics = topics;
	}

	protected String getGroupId() {
		return this.groupId;
	}

	/**
	 * Set the group.id property for the consumer.
	 * @param groupId the group id.
	 */
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	protected String getClientId() {
		return this.clientId;
	}

	/**
	 * Set the client.id property for the consumer.
	 * @param clientId the client id.
	 */
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	protected long getPollTimeout() {
		return this.pollTimeout;
	}

	/**
	 * Set the pollTimeout for the poll() operations; default 50ms.
	 * @param pollTimeout the poll timeout.
	 */
	public void setPollTimeout(long pollTimeout) {
		this.pollTimeout = pollTimeout;
	}

	protected RecordMessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	/**
	 * Set the messsage converter to replace the default
	 * {@link MessagingMessageConverter}.
	 * @param messageConverter the converter.
	 */
	public void setMessageConverter(RecordMessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	protected Type getPayloadType() {
		return this.payloadType;
	}

	/**
	 * Set the payload type.
	 * Only applies if a type-aware message converter is provided.
	 * @param payloadType the type to convert to.
	 */
	public void setPayloadType(Type payloadType) {
		this.payloadType = payloadType;
	}

	@Override
	public String getComponentType() {
		return "kafka:message-source";
	}

	@Override
	protected synchronized AbstractIntegrationMessageBuilder<Object> doReceive() {
		if (this.consumer == null) {
			createConsumer();
		}
		ConsumerRecord<K, V> record;
		if (this.recordIterator != null && this.recordIterator.hasNext()) {
			record = this.recordIterator.next();
		}
		else {
			try {
				Set<TopicPartition> paused = this.consumer.paused();
				if (paused.size() > 0) {
					this.consumer.resume(paused);
				}
				ConsumerRecords<K, V> records = this.consumer.poll(this.pollTimeout);
				this.consumer.pause(this.partitions);
				if (records == null || records.count() == 0) {
					this.recordIterator = null;
					return null;
				}
				this.recordIterator = records.iterator();
				record = this.recordIterator.next();
			}
			catch (WakeupException e) {
				return null;
			}
		}
		@SuppressWarnings("unchecked")
		Message<Object> message = (Message<Object>) this.messageConverter.toMessage(record, null, null,
				this.payloadType);
		AcknowledgmentCallback ackCallback = this.ackCallbackFactory
				.createCallback(new KafkaAckInfo<K, V>(this.consumer, record, this::resetIterator));
		return getMessageBuilderFactory().fromMessage(message)
				.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, ackCallback);
	}

	protected void createConsumer() {
		this.consumer = this.consumerFactory.createConsumer(this.groupId, this.clientId, null);
		this.consumer.subscribe(Arrays.asList(this.topics), new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				KafkaMessageSource.this.partitions = Collections.emptyList();
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				KafkaMessageSource.this.partitions = new ArrayList<>(partitions);
			}

		});
	}

	public synchronized void resetIterator() {
		this.recordIterator = null;
	}

	@Override
	public synchronized void destroy() throws Exception {
		if (this.consumer != null) {
			Consumer<K, V> consumer2 = this.consumer;
			this.consumer = null;
			consumer2.close(30, TimeUnit.SECONDS);
		}
	}

	/**
	 * AcknowledgmentCallbackFactory for KafkaAckInfo.
	 *
	 * @param <K> the key type.
	 * @param <V> the value type.
	 *
	 */
	public static class KafkaAckCallbackFactory<K, V> implements AcknowledgmentCallbackFactory<KafkaAckInfo<K, V>> {

		@Override
		public AcknowledgmentCallback createCallback(KafkaAckInfo<K, V> info) {
			return new KafkaAckCallback<K, V>(info);
		}

	}

	/**
	 * AcknowledgmentCallback for Kafka.
	 *
	 * @param <K> the key type.
	 * @param <V> the value type.
	 *
	 */
	public static class KafkaAckCallback<K, V> implements AcknowledgmentCallback {

		private final KafkaAckInfo<K, V> ackInfo;

		private volatile boolean acknowledged;

		public KafkaAckCallback(KafkaAckInfo<K, V> ackInfo) {
			this.ackInfo = ackInfo;
		}

		@Override
		public void acknowledge(Status status) {
			Assert.notNull(status, "'status' cannot be null");
			try {
				switch (status) {
				case ACCEPT:
				case REJECT:
					this.ackInfo.getConsumer().commitSync(Collections.singletonMap(new TopicPartition(
							this.ackInfo.getRecord().topic(), this.ackInfo.getRecord().partition()),
							new OffsetAndMetadata(this.ackInfo.getRecord().offset() + 1)));
					break;
				case REQUEUE:
					this.ackInfo.getConsumer().seek(
							new TopicPartition(this.ackInfo.getRecord().topic(), this.ackInfo.getRecord().partition()),
							this.ackInfo.getRecord().offset());
					this.ackInfo.getReset().reset();
					break;
				default:
					break;
				}
			}
			catch (WakeupException e) {
				throw new IllegalStateException(e);
			}
			this.acknowledged = true;
		}

		@Override
		public boolean isAcknowledged() {
			return this.acknowledged;
		}

	}

	/**
	 * Information for building an KafkaAckCallback.
	 *
	 * @param <K> the key type.
	 * @param <V> the value type.
	 *
	 */
	public static class KafkaAckInfo<K, V> {

		private final Consumer<K, V> consumer;

		private final ConsumerRecord<K, V> record;

		private final Reset reset;

		public KafkaAckInfo(Consumer<K, V> consumer, ConsumerRecord<K, V> record, Reset reset) {
			this.consumer = consumer;
			this.record = record;
			this.reset = reset;
		}

		public Consumer<K, V> getConsumer() {
			return this.consumer;
		}

		public ConsumerRecord<K, V> getRecord() {
			return this.record;
		}

		public Reset getReset() {
			return this.reset;
		}

	}

	/**
	 * Callback for resetting the iterator.
	 *
	 */
	@FunctionalInterface
	interface Reset {

		void reset();

	}

}

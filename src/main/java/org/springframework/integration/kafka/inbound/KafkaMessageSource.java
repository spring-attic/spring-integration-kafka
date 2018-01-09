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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.endpoint.AbstractMessageSource;
import org.springframework.integration.support.AbstractIntegrationMessageBuilder;
import org.springframework.integration.support.AcknowledgmentCallback;
import org.springframework.integration.support.AcknowledgmentCallbackFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaResourceHolder;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.ProducerFactoryUtils;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.KafkaMessageHeaders;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.transaction.support.ResourceHolderSynchronization;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

/**
 * Polled message source for kafka. Only one thread can poll for data (or
 * acknowledge a message) at a time.
 * <p>
 * NOTE: If the application acknowledges messages out of order, the acks
 * will be deferred until all messages prior to the offset are ack'd.
 * If multiple records a retrieved and an earlier offset is requeued, records
 * from the subsequent offsets will be redelivered - even if they were
 * processed successfully. Applications should therefore implement
 * idempotency.
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

	/**
	 * Exposes a {@link ProducerFactory} as a message header so that the application
	 * can participate in a transaction.
	 */
	public static final String PRODUCER_FACTORY = "kafka_producerFactory";

	private final ConsumerFactory<K, V> consumerFactory;

	private final KafkaAckCallbackFactory<K, V> ackCallbackFactory;

	private final String[] topics;

	private final Object consumerMonitor = new Object();

	private final Map<TopicPartition, Set<KafkaAckInfo<K, V>>> inflightRecords = new HashMap<>();

	private String groupId;

	private String clientId = "message.source";

	private long pollTimeout = DEFAULT_POLL_TIMEOUT;

	private RecordMessageConverter messageConverter = new MessagingMessageConverter();

	private Type payloadType;

	private ProducerFactory<K, V> producerFactory;

	private volatile Consumer<K, V> consumer;

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
		Object maxPoll = consumerFactory.getConfigurationProperties().get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
		if (maxPoll == null || (maxPoll instanceof Number && ((Number) maxPoll).intValue() != 1)
				|| (maxPoll instanceof String && Integer.parseInt((String) maxPoll) != 1)) {
			if (this.logger.isWarnEnabled()) {
				this.logger.warn("It is advisable to set " + ConsumerConfig.MAX_POLL_RECORDS_CONFIG
					+ " to 1 to avoid having to seek after each record");
			}
		}
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
	 * Set the message converter to replace the default
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

	protected ProducerFactory<K, V> getTransactionalProducerFactory() {
		return this.producerFactory;
	}

	/**
	 * Set a transactional producer factory.
	 * @param producerFactory the producer factory.
	 */
	public void setTransactionalProducerFactory(ProducerFactory<K, V> producerFactory) {
		Assert.notNull(producerFactory, "The producer factory cannot be null");
		Assert.isTrue(producerFactory.transactionCapable(), "The producer factory must be transaction capable");
		this.producerFactory = producerFactory;
	}

	@Override
	public String getComponentType() {
		return "kafka:message-source";
	}

	@Override
	protected synchronized Object doReceive() {
		if (this.consumer == null) {
			createConsumer();
		}
		ConsumerRecord<K, V> record;
		TopicPartition topicPartition;
		synchronized (this.consumerMonitor) {
			Set<TopicPartition> paused = this.consumer.paused();
			if (paused.size() > 0) {
				this.consumer.resume(paused);
			}
			ConsumerRecords<K, V> records = this.consumer.poll(this.pollTimeout);
			this.consumer.pause(this.partitions);
			if (records == null || records.count() == 0) {
				return null;
			}
			record = records.iterator().next();
			topicPartition = new TopicPartition(record.topic(), record.partition());
			if (records.count() > 1) {
				this.consumer.seek(topicPartition, record.offset() + 1);
			}
		}
		Producer<K, V> producer = null;
		AtomicBoolean myTransaction = new AtomicBoolean();
		AtomicReference<Producer<K, V>> producerRef = new AtomicReference<>();
		KafkaAckInfo<K, V> ackInfo = new KafkaAckInfo<K, V>(this.consumerMonitor, this.groupId, this.consumer, record,
				topicPartition, producerRef, this.inflightRecords, myTransaction);
		if (this.producerFactory != null) {
			@SuppressWarnings("unchecked")
			KafkaResourceHolder<K, V> holder = (KafkaResourceHolder<K, V>) TransactionSynchronizationManager
					.getResource(this.producerFactory);
			if (holder == null) {
				holder = createTransaction(ackInfo);
				myTransaction.set(true);
			}
			producer = holder.getProducer();
			producerRef.set(producer);
		}
		AcknowledgmentCallback ackCallback = this.ackCallbackFactory.createCallback(ackInfo);
		this.inflightRecords.computeIfAbsent(topicPartition, tp -> new TreeSet<>()).add(ackInfo);
		@SuppressWarnings("unchecked")
		Message<Object> message = (Message<Object>) this.messageConverter.toMessage(record,
				ackCallback instanceof Acknowledgment ? (Acknowledgment) ackCallback : null, this.consumer,
				this.payloadType);
		if (message.getHeaders() instanceof KafkaMessageHeaders) {
			Map<String, Object> rawHeaders = ((KafkaMessageHeaders) message.getHeaders()).getRawHeaders();
			rawHeaders.put(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, ackCallback);
			if (this.producerFactory != null) {
				rawHeaders.put(PRODUCER_FACTORY, ackInfo);
			}
			return message;
		}
		else {
			AbstractIntegrationMessageBuilder<Object> builder = getMessageBuilderFactory().fromMessage(message)
					.setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, ackCallback);
			if (this.producerFactory != null) {
				builder.setHeader(PRODUCER_FACTORY, ackInfo);
			}
			return builder;
		}
	}

	private KafkaResourceHolder<K, V> createTransaction(KafkaAckInfo<K, V> ackInfo) {
		Producer<K, V> producer = this.producerFactory.createProducer();
		producer.beginTransaction();
		KafkaResourceHolder<K, V> resourceHolder = new KafkaResourceHolder<K, V>(producer);
		TransactionSynchronizationManager.bindResource(ackInfo, resourceHolder);
		resourceHolder.setSynchronizedWithTransaction(true);
		if (TransactionSynchronizationManager.isSynchronizationActive()) {
			TransactionSynchronizationManager
					.registerSynchronization(new KafkaResourceSynchronization<K, V>(resourceHolder, ackInfo));
		}
		return resourceHolder;
	}

	protected void createConsumer() {
		this.consumer = this.consumerFactory.createConsumer(this.groupId, this.clientId, null);
		synchronized (this.consumerMonitor) {
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
	}

	@Override
	public synchronized void destroy() throws Exception {
		if (this.consumer != null) {
			Consumer<K, V> consumer2 = this.consumer;
			this.consumer = null;
			synchronized (this.consumerMonitor) {
				consumer2.close(30, TimeUnit.SECONDS);
			}
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
	public static class KafkaAckCallback<K, V> implements AcknowledgmentCallback, Acknowledgment {

		private final Log logger = LogFactory.getLog(getClass());

		private final KafkaAckInfo<K, V> ackInfo;

		private volatile boolean acknowledged;

		private boolean autoAckEnabled = true;

		public KafkaAckCallback(KafkaAckInfo<K, V> ackInfo) {
			this.ackInfo = ackInfo;
		}

		@Override
		public void acknowledge(Status status) {
			Assert.notNull(status, "'status' cannot be null");
			if (this.acknowledged) {
				throw new IllegalStateException("Already acknowledged");
			}
			synchronized (this.ackInfo.getConsumerMonitor()) {
				try {
					ConsumerRecord<K, V> record = this.ackInfo.getRecord();
					switch (status) {
					case ACCEPT:
					case REJECT:
						commitIfPossible(record);
						break;
					case REQUEUE:
						rollback(record);
						break;
					default:
						break;
					}
				}
				catch (WakeupException e) {
					throw new IllegalStateException(e);
				}
				finally {
					this.acknowledged = true;
					if (!this.ackInfo.isAckDeferred()) {
						this.ackInfo.getOffsets().get(this.ackInfo.getTopicPartition()).remove(this.ackInfo);
					}
				}
			}
		}

		private void rollback(ConsumerRecord<K, V> record) {
			if (this.ackInfo.isMyTransaction()) {
				this.ackInfo.getProducer().abortTransaction();
				this.ackInfo.getProducer().close();
				TransactionSynchronizationManager.unbindResource(this.ackInfo);
			}
			this.ackInfo.getConsumer().seek(this.ackInfo.getTopicPartition(), record.offset());
			Set<KafkaAckInfo<K, V>> inflight = this.ackInfo.getOffsets().get(this.ackInfo.getTopicPartition());
			if (inflight.size() > 1) {
				List<Long> rewound =
						inflight.stream()
							.filter(i -> i.getRecord().offset() > record.offset())
							.peek(i -> {
								i.setRolledBack(true);
							})
							.map(i -> i.getRecord().offset())
							.collect(Collectors.toList());
				if (rewound.size() > 0 && this.logger.isWarnEnabled()) {
					this.logger.warn("Rolled back " + record + " later in-flight offsets "
							+ rewound + " will also be re-fetched");
				}
			}
		}

		private void commitIfPossible(ConsumerRecord<K, V> record) {
			if (this.ackInfo.isRolledBack()) {
				if (this.logger.isWarnEnabled()) {
					this.logger.warn("Cannot send offset for " + record
							+ " to transaction; an earlier offset was rolled back");
				}
			}
			else {
				Set<KafkaAckInfo<K, V>> candidates = this.ackInfo.getOffsets().get(this.ackInfo.getTopicPartition());
				KafkaAckInfo<K, V> ackInfo = null;
				if (candidates.iterator().next().equals(this.ackInfo)) {
					// see if there are any pending acks for higher offsets
					List<KafkaAckInfo<K, V>> toCommit = new ArrayList<>();
					for (KafkaAckInfo<K, V> info : candidates) {
						if (info != this.ackInfo) {
							if (info.isAckDeferred()) {
								toCommit.add(info);
							}
							else {
								break;
							}
						}
					}
					if (toCommit.size() > 0) {
						ackInfo = toCommit.get(toCommit.size() - 1);
						if (this.logger.isDebugEnabled()) {
							this.logger.debug("Committing pending offsets for " + record + " and all deferred to "
									+ ackInfo.getRecord());
						}
						candidates.removeAll(toCommit);
					}
					else {
						ackInfo = this.ackInfo;
					}
				}
				else { // earlier offsets present
					this.ackInfo.setAckDeferred(true);
				}
				if (ackInfo != null) {
					if (ackInfo.getProducer() != null) {
						ackInfo.getProducer().sendOffsetsToTransaction(
								Collections.singletonMap(ackInfo.getTopicPartition(),
										new OffsetAndMetadata(ackInfo.getRecord().offset() + 1)),
								ackInfo.getGroupId());
					}
					else {
						ackInfo.getConsumer().commitSync(Collections.singletonMap(ackInfo.getTopicPartition(),
								new OffsetAndMetadata(ackInfo.getRecord().offset() + 1)));
					}
				}
				else {
					if (this.logger.isDebugEnabled()) {
						this.logger.debug("Deferring commit offset; earlier messages are in flight.");
					}
				}
			}
			if (this.ackInfo.isMyTransaction()) {
				this.ackInfo.getProducer().commitTransaction();
				this.ackInfo.getProducer().close();
				TransactionSynchronizationManager.unbindResource(this.ackInfo);
			}
		}

		@Override
		public boolean isAcknowledged() {
			return this.acknowledged;
		}

		@Override
		public void acknowledge() {
			acknowledge(Status.ACCEPT);
		}

		@Override
		public void noAutoAck() {
			this.autoAckEnabled = false;
		}

		@Override
		public boolean isAutoAck() {
			return this.autoAckEnabled;
		}

	}

	/**
	 * Information for building an KafkaAckCallback.
	 *
	 * @param <K> the key type.
	 * @param <V> the value type.
	 *
	 */
	public static class KafkaAckInfo<K, V> implements Comparable<KafkaAckInfo<K, V>>, ProducerFactory<K, V> {

		private final Object consumerMonitor;

		private final String groupId;

		private final Consumer<K, V> consumer;

		private final ConsumerRecord<K, V> record;

		private final TopicPartition topicPartition;

		private final AtomicReference<Producer<K, V>> producer;

		private final Map<TopicPartition, Set<KafkaAckInfo<K, V>>> offsets;

		private final AtomicBoolean myTransaction;

		private volatile boolean rolledBack;

		private volatile boolean ackDeferred;

		KafkaAckInfo(Object consumerMonitor, String groupId, Consumer<K, V> consumer, ConsumerRecord<K, V> record,
				TopicPartition topicPartition, AtomicReference<Producer<K, V>> producerRef,
				Map<TopicPartition, Set<KafkaAckInfo<K, V>>> offsets, AtomicBoolean myTransaction) {
			this.consumerMonitor = consumerMonitor;
			this.groupId = groupId;
			this.consumer = consumer;
			this.record = record;
			this.topicPartition = topicPartition;
			this.producer = producerRef;
			this.offsets = offsets;
			this.myTransaction = myTransaction;
		}

		Object getConsumerMonitor() {
			return this.consumerMonitor;
		}

		public String getGroupId() {
			return this.groupId;
		}

		public Consumer<K, V> getConsumer() {
			return this.consumer;
		}

		public ConsumerRecord<K, V> getRecord() {
			return this.record;
		}

		public TopicPartition getTopicPartition() {
			return this.topicPartition;
		}

		public Producer<K, V> getProducer() {
			return this.producer.get();
		}

		public Map<TopicPartition, Set<KafkaAckInfo<K, V>>> getOffsets() {
			return this.offsets;
		}

		public boolean isRolledBack() {
			return this.rolledBack;
		}

		public void setRolledBack(boolean rolledBack) {
			this.rolledBack = rolledBack;
		}

		public boolean isAckDeferred() {
			return this.ackDeferred;
		}

		public void setAckDeferred(boolean ackDeferred) {
			this.ackDeferred = ackDeferred;
		}

		public boolean isMyTransaction() {
			return this.myTransaction.get();
		}

		@Override
		public int compareTo(KafkaAckInfo<K, V> other) {
			return Long.compare(this.record.offset(), other.getRecord().offset());
		}

		@Override
		public String toString() {
			return "KafkaAckInfo [record=" + this.record + ", rolledBack=" + this.rolledBack + ", ackDeferred="
					+ this.ackDeferred + ", myTransaction=" + this.myTransaction + "]";
		}

		@Override
		public Producer<K, V> createProducer() {
			Assert.state(this.producer.get() != null, "There is no transactional producer here");
			return this.producer.get();
		}

		@Override
		public boolean transactionCapable() {
			return this.producer.get() != null;
		}

	}

	/**
	 * Callback for resource cleanup at the end of a non-native Kafka transaction (e.g. when participating in a
	 * JtaTransactionManager transaction).
	 * TODO: Make the class in PFU public
	 * @see org.springframework.transaction.jta.JtaTransactionManager
	 */
	private static final class KafkaResourceSynchronization<K, V> extends
			ResourceHolderSynchronization<KafkaResourceHolder<K, V>, Object> {

		private final KafkaResourceHolder<K, V> resourceHolder;

		KafkaResourceSynchronization(KafkaResourceHolder<K, V> resourceHolder, Object resourceKey) {
			super(resourceHolder, resourceKey);
			this.resourceHolder = resourceHolder;
		}

		@Override
		protected boolean shouldReleaseBeforeCompletion() {
			return false;
		}

		@Override
		public void afterCompletion(int status) {
			if (status == TransactionSynchronization.STATUS_COMMITTED) {
				this.resourceHolder.commit();
			}
			else {
				this.resourceHolder.rollback();
			}

			super.afterCompletion(status);
		}

		@Override
		protected void releaseResource(KafkaResourceHolder<K, V> resourceHolder, Object resourceKey) {
			ProducerFactoryUtils.releaseResources(resourceHolder);
		}

	}

}

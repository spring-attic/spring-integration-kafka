/*
 * Copyright 2015-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.util.Assert;

/**
 * Message listener container using the Java {@link Consumer} supporting
 * auto-partition assignment or user-configured assignment.
 * <p>
 * With the latter, initial partition offsets can be provided.
 *
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public class KafkaMessageListenerContainer<K, V> extends AbstractMessageListenerContainer<K, V> {

	private static final int DEFAULT_STOP_TIMEOUT = 1000;

	final Log logger = LogFactory.getLog(this.getClass());

	private final ConsumerFactory<K, V> consumerFactory;

	private final String[] topics;

	private final Pattern topicPattern;

	final List<FetchTask> fetchTasks = new ArrayList<>();

	private int ackCount;

	private long ackTime;

	private ContainerOffsetResetStrategy resetStrategy = ContainerOffsetResetStrategy.NONE;

	private long recentOffset = 1;


	TopicPartition[] partitions;

	Executor fetchTaskExecutor;

	int concurrency = 1;

	private int stopTimeout = DEFAULT_STOP_TIMEOUT;

	private ErrorHandler errorHandler = new LoggingErrorHandler();

	/**
	 * Construct an instance with the supplied configuration properties and specific
	 * topics/partitions - when using this constructor, a
	 * {@link #setResetStrategy(ContainerOffsetResetStrategy)} can be used.
	 * @param consumerFactory the consumer factory.
	 * @param topicPartitions the topics/partitions.
	 */
	public KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory, TopicPartition... topicPartitions) {
		Assert.notNull(consumerFactory, "A ConsumerFactory must be provided");
		Assert.notEmpty(topicPartitions, "A list of partitions must be provided");
		Assert.noNullElements(topicPartitions, "The list of partitions cannot contain null elements");
		this.consumerFactory = consumerFactory;
		this.partitions = topicPartitions;
		this.topics = null;
		this.topicPattern = null;
	}

	/**
	 * Construct an instance with the supplied configuration properties and topics.
	 * When using this constructor, a
	 * {@link #setResetStrategy(ContainerOffsetResetStrategy)} cannot be used.
	 * @param consumerFactory the consumer factory.
	 * @param topics the topics.
	 */
	public KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory, String... topics) {
		Assert.notNull(consumerFactory, "A ConsumerFactory must be provided");
		Assert.notNull(topics, "A list of topics must be provided");
		Assert.noNullElements(topics, "The list of topics cannot contain null elements");
		this.consumerFactory = consumerFactory;
		this.topics = topics;
		this.topicPattern = null;
	}

	/**
	 * Construct an instance with the supplied configuration properties and topic
	 * pattern. When using this constructor, a
	 * {@link #setResetStrategy(ContainerOffsetResetStrategy)} cannot be used.
	 * @param consumerFactory the consumer factory.
	 * @param topicPattern the topic pattern.
	 */
	public KafkaMessageListenerContainer(ConsumerFactory<K, V> consumerFactory, Pattern topicPattern) {
		Assert.notNull(consumerFactory, "A ConsumerFactory must be provided");
		Assert.notNull(topicPattern, "A topic pattern must be provided");
		this.consumerFactory = consumerFactory;
		this.topics = null;
		this.topicPattern = topicPattern;
	}

	/**
	 * The initial offset reset strategy, when explicit partitions are provided.
	 * <ul>
	 * <li>NONE: No reset</li>
	 * <li>EARLIEST: Set to the earliest message</li>
	 * <li>LATEST: Set to the last message; receive new messages only</li>
	 * <li>RECENT: Set to a recent message based on {@link #setRecentOffset(long) recentOffset}</li>
	 * </ul>
	 *
	 * @param resetStrategy the {@link ContainerOffsetResetStrategy}
	 */
	public void setResetStrategy(ContainerOffsetResetStrategy resetStrategy) {
		this.resetStrategy = resetStrategy;
	}

	/**
	 * Set the number of records back from the latest when using
	 * {@link ContainerOffsetResetStrategy#RECENT}.
	 * @param recentOffset the offset from the latest; default 1.
	 */
	public void setRecentOffset(long recentOffset) {
		this.recentOffset = recentOffset;
	}

	public ErrorHandler getErrorHandler() {
		return errorHandler;
	}

	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	public int getConcurrency() {
		return concurrency;
	}

	/**
	 * The maximum number of concurrent {@link MessageListener}s running. Messages from
	 * within the same partition will be processed sequentially.
	 * Concurrency must be 1 when explicitly suppplying partitions.
	 * @param concurrency the concurrency maximum number
	 */
	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}

	/**
	 * The timeout for waiting for each concurrent {@link MessageListener} to finish on
	 * stopping.
	 * @param stopTimeout timeout in milliseconds
	 * @since 1.1
	 */
	public void setStopTimeout(int stopTimeout) {
		this.stopTimeout = stopTimeout;
	}

	public int getStopTimeout() {
		return stopTimeout;
	}

	public Executor getFetchTaskExecutor() {
		return fetchTaskExecutor;
	}

	/**
	 * The task executor for fetch operations.
	 * @param fetchTaskExecutor the Executor for fetch operations
	 */
	public void setFetchTaskExecutor(Executor fetchTaskExecutor) {
		this.fetchTaskExecutor = fetchTaskExecutor;
	}

	/*
	 * Under lifecycle lock.
	 */
	@Override
	protected void doStart() {
		if (!isRunning()) {
			if (this.fetchTaskExecutor == null) {
				this.fetchTaskExecutor = new SimpleAsyncTaskExecutor(
						(getBeanName() == null ? "" : getBeanName() + "-") + "kafka-fetch-");
			}
			if (this.partitions != null && this.concurrency > 1) {
				logger.warn("When specific partitions are provided, the concurrency must be 1 to avoid "
						+ "duplicate message delivery; reduced from " + this.concurrency + " to 1");
				this.concurrency = 1;
			}
			setRunning(true);
			for (int i = 0; i < this.concurrency; i++) {
				FetchTask fetchTask = new FetchTask();
				this.fetchTasks.add(fetchTask);
				fetchTaskExecutor.execute(fetchTask);
			}
		}
	}

	/*
	 * Under lifecycle lock.
	 */
	@Override
	protected void doStop() {
		if (isRunning()) {
			setRunning(false);
			for (FetchTask fetchTask : this.fetchTasks) {
				fetchTask.stop();
			}
		}
	}

	public class FetchTask implements SchedulingAwareRunnable {

		private final Consumer<K, V> consumer;

		private final boolean autoCommit;

		private final Collection<ConsumerRecords<K, V>> unAcked = new ArrayList<>();

		private final Collection<ConsumerRecord<K, V>> manualAcks = new LinkedList<>();

		private final CountDownLatch assignmentLatch = new CountDownLatch(1);

		private MessageListener<K, V> listener;

		private AcknowledgingMessageListener<K, V> acknowledgingMessageListener;

		private volatile Collection<TopicPartition> topicPartitions;

		@SuppressWarnings("unchecked")
		public FetchTask() {
			Object messageListener = getMessageListener();
			Assert.state(messageListener != null, "A MessageListener is required");
			if (messageListener instanceof AcknowledgingMessageListener) {
				this.acknowledgingMessageListener = (AcknowledgingMessageListener<K, V>) messageListener;
			}
			else if (messageListener instanceof MessageListener) {
				this.listener = (MessageListener<K, V>) messageListener;
			}
			else {
				throw new IllegalStateException("messageListener must be 'MessageListener' "
						+ "or 'AcknowledgingMessageListener', not " + messageListener.getClass().getName());
			}
			this.autoCommit = consumerFactory.isAutoCommit();
			Consumer<K, V> consumer = consumerFactory.createConsumer();
			ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {

				@Override
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					logger.info("partitions revoked:" + partitions);
				}

				@Override
				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					topicPartitions = partitions;
					assignmentLatch.countDown();
					logger.info("partitions assigned:" + partitions);
				}

			};
			if (partitions == null) {
				if (topicPattern != null) {
					consumer.subscribe(topicPattern, rebalanceListener);
				}
				else {
					consumer.subscribe(Arrays.asList(topics), rebalanceListener);
				}
			}
			else {
				List<TopicPartition> topicPartitions = Arrays.asList(partitions);
				this.topicPartitions = topicPartitions;
				consumer.assign(topicPartitions);
			}
			this.consumer = consumer;
		}

		@Override
		public void run() {
			int count = 0;
			long last = System.currentTimeMillis();
			long now;
			class CommitCallback implements OffsetCommitCallback {

				@Override
				public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
					if (exception != null) {
						logger.error("Commit failed for " + offsets, exception);
					}
					else if (logger.isDebugEnabled()) {
						logger.debug("Commits for " + offsets + " completed");
					}
 				}

			}
			CommitCallback callback = new CommitCallback();
			if (isRunning() && this.topicPartitions != null) {
				/*
				 * Note: initial position setting is only supported with explicit topic assignment.
				 * When using auto assignment (subscribe), the ConsumerRebalanceListener is not
				 * called until we poll() the consumer.
				 */
				if (resetStrategy.equals(ContainerOffsetResetStrategy.EARLIEST)) {
					this.consumer.seekToBeginning(
							this.topicPartitions.toArray(new TopicPartition[this.topicPartitions.size()]));
				}
				else if (resetStrategy.equals(ContainerOffsetResetStrategy.LATEST)) {
					this.consumer.seekToEnd(
							this.topicPartitions.toArray(new TopicPartition[this.topicPartitions.size()]));
				}
				else if (resetStrategy.equals(ContainerOffsetResetStrategy.RECENT)) {
					this.consumer.seekToEnd(
							this.topicPartitions.toArray(new TopicPartition[this.topicPartitions.size()]));
					for (TopicPartition topicPartition : this.topicPartitions) {
						long newOffset = this.consumer.position(topicPartition) - recentOffset;
						this.consumer.seek(topicPartition, newOffset);
						if (logger.isDebugEnabled()) {
							logger.debug("Reset " + topicPartition + " to offset " + newOffset);
						}
					}
				}
			}
			while (isRunning()) {
				try {
					if (logger.isTraceEnabled()) {
						logger.trace("Polling...");
					}
					ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
					if (records != null) {
						count += records.count();
						if (logger.isDebugEnabled()) {
							logger.debug("Received: " + records.count() + " records");
						}
						Iterator<ConsumerRecord<K, V>> iterator = records.iterator();
						while (iterator.hasNext()) {
							final ConsumerRecord<K, V> record = iterator.next();
							if (this.acknowledgingMessageListener != null) {
								this.acknowledgingMessageListener.onMessage(record, new Acknowledgment() {

									@Override
									public void acknowledge() {
										manualAcks.add(record);
									}

								});
							}
							else {
								this.listener.onMessage(record);
							}
							if (!this.autoCommit && ackMode.equals(AckMode.RECORD)) {
								this.consumer.commitAsync(
										Collections.singletonMap(new TopicPartition(record.topic(), record.partition()),
												new OffsetAndMetadata(record.offset())), callback);
							}
						}
						if (!this.autoCommit && !records.isEmpty()) {
							if (ackMode.equals(AckMode.BATCH)) {
								this.consumer.commitAsync(callback);
							}
							else if (ackMode.equals(AckMode.COUNT) && count >= ackCount) {
 								this.consumer.commitAsync(buildCommitMap(), callback);
 								count = 0;
							}
							else if (ackMode.equals(AckMode.TIME)
									&& (now = System.currentTimeMillis()) - last > ackTime) {
 								this.consumer.commitAsync(buildCommitMap(), callback);
								last = now;
								this.unAcked.clear();
							}
							else if (!ackMode.equals(AckMode.MANUAL)) {
								this.unAcked.add(records);
							}
						}
						// TODO: manual acks
					}
					else {
						if (logger.isDebugEnabled()) {
							logger.debug("No records");
						}
					}
				}
				catch (WakeupException e) {
					;
				}
			}
			if (this.unAcked.size() > 0) {
				this.consumer.commitSync(buildCommitMap());
			}
			this.consumer.close();
		}

		private Map<TopicPartition, OffsetAndMetadata> buildCommitMap() {
			Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
			for (ConsumerRecords<K, V> recordsToAck : this.unAcked) {
				Iterator<ConsumerRecord<K, V>> ackIterator = recordsToAck.iterator();
				while (ackIterator.hasNext()) {
					ConsumerRecord<K, V> next = ackIterator.next();
					commits.put(new TopicPartition(next.topic(), next.partition()),
							new OffsetAndMetadata(next.offset()));
				}
			}
			this.unAcked.clear();
			return commits;
		}

		@Override
		public boolean isLongLived() {
			return true;
		}

		void stop() {
			this.consumer.wakeup();
		}

	}

	public enum ContainerOffsetResetStrategy {
		LATEST, EARLIEST, NONE, RECENT
	}

}

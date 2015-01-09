/*
 * Copyright 2014 the original author or authors.
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


package org.springframework.integration.kafka.listener;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.gs.collections.api.RichIterable;
import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.block.procedure.Procedure2;
import com.gs.collections.api.list.ImmutableList;
import com.gs.collections.api.list.MutableList;
import com.gs.collections.api.map.MutableMap;
import com.gs.collections.impl.block.factory.Functions;
import com.gs.collections.impl.block.function.checked.CheckedFunction;
import com.gs.collections.impl.factory.Lists;
import com.gs.collections.impl.utility.ArrayIterate;

import org.springframework.context.SmartLifecycle;
import org.springframework.integration.kafka.core.KafkaBrokerAddress;
import org.springframework.integration.kafka.core.KafkaBrokerConnectionFactory;
import org.springframework.integration.kafka.core.KafkaMessage;
import org.springframework.integration.kafka.core.KafkaMessageBatch;
import org.springframework.integration.kafka.core.KafkaMessageFetchRequest;
import org.springframework.integration.kafka.core.KafkaTemplate;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.util.Assert;

/**
 * @author Marius Bogoevici
 */
public class KafkaMessageListenerContainer implements SmartLifecycle {

	private final GetOffsetForPartition getOffset = new GetOffsetForPartition();

	private final GetLeaderFunction getLeader = new GetLeaderFunction();

	private final Function<Partition, Partition> passThru = Functions.getPassThru();

	private final LaunchFetchTaskProcedure launchFetchTask = new LaunchFetchTaskProcedure();

	private final Object lifecycleMonitor = new Object();

	private final KafkaTemplate kafkaTemplate;

	private final ImmutableList<Partition> partitions;

	public boolean autoStartup = true;

	private Executor taskExecutor;

	private int concurrency = 1;

	private volatile boolean running = false;

	private long timeout = 100L;

	private int maxSize = 10000;

	private MessageListener messageListener;

	private volatile OffsetManager offsetManager;

	private ConcurrentMap<Partition, Long> fetchOffsets;

	private ConcurrentMessageListenerDispatcher messageDispatcher;


	public KafkaMessageListenerContainer(KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory, Partition[] partitions) {
		Assert.notNull(kafkaBrokerConnectionFactory, "A connection factory must be supplied");
		Assert.notEmpty(partitions, "A list of partitions must be provided");
		this.kafkaTemplate = new KafkaTemplate(kafkaBrokerConnectionFactory);
		this.partitions = Lists.immutable.with(partitions);
	}

	public KafkaMessageListenerContainer(final KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory, String[] topics) {
		this(kafkaBrokerConnectionFactory, getPartitionsForTopics(kafkaBrokerConnectionFactory, topics));
	}

	private static Partition[] getPartitionsForTopics(final KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory, String[] topics) {
		MutableList<Partition> partitionList = ArrayIterate.flatCollect(topics, new GetPartitionsForTopic(kafkaBrokerConnectionFactory));
		return partitionList.toArray(new Partition[partitionList.size()]);
	}

	public OffsetManager getOffsetManager() {
		return offsetManager;
	}

	public void setOffsetManager(OffsetManager offsetManager) {
		this.offsetManager = offsetManager;
	}

	public MessageListener getMessageListener() {
		return messageListener;
	}

	public void setMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
	}

	public int getConcurrency() {
		return concurrency;
	}

	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}

	public int getMaxSize() {
		return maxSize;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	@Override
	public boolean isAutoStartup() {
		return autoStartup;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	@Override
	public void stop(Runnable callback) {
		synchronized (lifecycleMonitor) {
			if (running) {
				this.running = false;
				this.messageDispatcher.stop();
			}
		}
	}

	@Override
	public void start() {
		synchronized (lifecycleMonitor) {
			if (!running) {
				this.running = true;
				if (this.offsetManager == null) {
					this.offsetManager = new MetadataStoreOffsetManager(kafkaTemplate.getKafkaBrokerConnectionFactory());
				}
				// initialize the fetch offset table - defer to OffsetManager for retrieving them
				this.fetchOffsets = new ConcurrentHashMap<Partition, Long>(this.partitions.toMap(passThru, getOffset));
				this.messageDispatcher = new ConcurrentMessageListenerDispatcher(messageListener, partitions.toArray(new Partition[partitions.size()]), concurrency, offsetManager);
				this.messageDispatcher.start();
				MutableMap<KafkaBrokerAddress, RichIterable<Partition>> partitionsByBrokerMap = this.partitions.groupBy(getLeader).toMap();
				if (taskExecutor == null) {
					taskExecutor = Executors.newFixedThreadPool(partitionsByBrokerMap.size());
				}
				partitionsByBrokerMap.forEachKeyValue(launchFetchTask);
			}
		}
	}

	@Override
	public void stop() {
		this.stop(null);
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public int getPhase() {
		return 0;
	}

	static class GetPartitionsForTopic extends CheckedFunction<String, Iterable<Partition>> {

		private final KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory;

		public GetPartitionsForTopic(KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory) {
			this.kafkaBrokerConnectionFactory = kafkaBrokerConnectionFactory;
		}

		@Override
		public Iterable<Partition> safeValueOf(String topic) throws Exception {
			return kafkaBrokerConnectionFactory.getPartitions(topic);
		}
	}

	/**
	 * Fetches data from Kafka for a group of partitions, located on the same broker.
	 */
	public class FetchTask implements Runnable {

		private MutableList<Partition> partitions;

		public FetchTask(MutableList<Partition> partition) {
			this.partitions = partition;
		}

		@Override
		public void run() {
			KafkaMessageListenerContainer kafkaMessageListenerContainer = KafkaMessageListenerContainer.this;
			while (running) {
				Set<Partition> partitionsWithRemainingData;
				do {
					partitionsWithRemainingData = new HashSet<Partition>();
					Iterable<KafkaMessageBatch> receive = kafkaTemplate.receive(this.partitions.collect(new Function<Partition, KafkaMessageFetchRequest>() {
						@Override
						public KafkaMessageFetchRequest valueOf(Partition partition) {
							return new KafkaMessageFetchRequest(partition, fetchOffsets.get(partition), maxSize);
						}
					}).toArray(new KafkaMessageFetchRequest[0]));
					for (KafkaMessageBatch batch : receive) {
						if (!batch.getMessages().isEmpty()) {
							long highestFetchedOffset = 0;
							for (KafkaMessage kafkaMessage : batch.getMessages()) {
								// fetch operations may return entire blocks of compressed messages, which may have lower offsets than the ones requested
								// thus a batch may contain messages that have been processed already
								if (kafkaMessage.getMetadata().getOffset() >= fetchOffsets.get(batch.getPartition())) {
									messageDispatcher.dispatch(kafkaMessage);
								}
								highestFetchedOffset = Math.max(highestFetchedOffset, kafkaMessage.getMetadata().getNextOffset());
							}
							fetchOffsets.replace(batch.getPartition(), highestFetchedOffset);
							// if there are still messages on server, we can go on and retrieve more
							if (highestFetchedOffset < batch.getHighWatermark()) {
								partitionsWithRemainingData.add(batch.getPartition());
							}
						}
					}
				} while (!partitionsWithRemainingData.isEmpty());
				try {
					Thread.currentThread().sleep(timeout);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	class GetOffsetForPartition extends CheckedFunction<Partition, Long> {
		@Override
		public Long safeValueOf(Partition object) throws Exception {
			return offsetManager.getOffset(object);
		}
	}

	private class GetLeaderFunction implements Function<Partition, KafkaBrokerAddress> {
		@Override
		public KafkaBrokerAddress valueOf(Partition partition) {
			return kafkaTemplate.getKafkaBrokerConnectionFactory().getLeader(partition);
		}
	}

	private class LaunchFetchTaskProcedure implements Procedure2<KafkaBrokerAddress, RichIterable<Partition>> {
		@Override
		public void value(KafkaBrokerAddress brokerAddress, RichIterable<Partition> partitions) {
			taskExecutor.execute(new FetchTask(partitions.toList()));
		}
	}
}

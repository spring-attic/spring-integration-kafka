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


package org.springframework.integration.kafka.core;

import static com.gs.collections.impl.utility.LazyIterate.collect;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.block.function.Function2;
import com.gs.collections.api.tuple.Pair;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.tuple.Tuples;
import com.gs.collections.impl.utility.LazyIterate;
import com.gs.collections.impl.utility.MapIterate;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.util.Assert;

/**
 * An connection to a Kafka broker - used mainly
 *
 * @author Marius Bogoevici
 */
public class KafkaBrokerConnection {

	public static final String DEFAULT_CLIENT_ID = "spring.kafka";

	public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

	public static final int DEFAULT_SOCKET_TIMEOUT = 10 * 1000;

	private static Log log = LogFactory.getLog(KafkaBrokerConnection.class);

	private final AtomicInteger correlationIdCounter = new AtomicInteger(new Random(new Date().getTime()).nextInt());

	private final SimpleConsumer simpleConsumer;

	private KafkaBrokerAddress brokerAddress;

	public KafkaBrokerConnection(KafkaBrokerAddress brokerAddress) {
		this(brokerAddress, DEFAULT_CLIENT_ID);
	}

	public KafkaBrokerConnection(KafkaBrokerAddress brokerAddress, String clientId) {
		this(brokerAddress, clientId, DEFAULT_BUFFER_SIZE, DEFAULT_SOCKET_TIMEOUT);
	}

	public KafkaBrokerConnection(KafkaBrokerAddress brokerAddress, String clientId, int bufferSize, int soTimeout) {
		this.brokerAddress = brokerAddress;
		this.simpleConsumer = new SimpleConsumer(brokerAddress.getHost(), brokerAddress.getPort(), soTimeout, bufferSize, clientId);
	}

	/**
	 * The broker address for this consumer
	 *
	 * @return
	 */
	public KafkaBrokerAddress getBrokerAddress() {
		return brokerAddress;
	}

	public void close() {
		this.simpleConsumer.close();
	}

	/**
	 * Fetches results
	 *
	 * @return a combination of messages and errors, depending on whether the invocation was successful or not
	 */
	public KafkaResult<KafkaMessageBatch> fetch(KafkaMessageFetchRequest... requests) {
		FetchRequestBuilder fetchRequestBuilder = new FetchRequestBuilder();
		for (KafkaMessageFetchRequest request : requests) {
			Partition partition = request.getPartition();
			long offset = request.getOffset();
			int maxSize = request.getMaxSize();
			fetchRequestBuilder.addFetch(partition.getTopic(), partition.getId(), offset, maxSize);
		}
		FetchResponse fetchResponse = this.simpleConsumer.fetch(fetchRequestBuilder.build());
		KafkaResultBuilder<KafkaMessageBatch> kafkaResultBuilder = new KafkaResultBuilder<KafkaMessageBatch>();
		for (final KafkaMessageFetchRequest request : requests) {
			Partition partition = request.getPartition();
			if (log.isDebugEnabled()) {
				log.debug("Reading from " + partition + "@" + request.getOffset());
			}
			short errorCode = fetchResponse.errorCode(partition.getTopic(), partition.getId());
			if (ErrorMapping.NoError() == errorCode) {
				ByteBufferMessageSet messageSet = fetchResponse.messageSet(partition.getTopic(), partition.getId());
				List<KafkaMessage> kafkaMessages = collect(messageSet, new ConvertToKafkaMessageFunction(request)).toList();
				long highWatermark = fetchResponse.highWatermark(partition.getTopic(), partition.getId());
				kafkaResultBuilder.add(partition).withResult(new KafkaMessageBatch(partition, kafkaMessages, highWatermark));
			}
			else {
				kafkaResultBuilder.add(partition).withError(errorCode);
			}
		}
		return kafkaResultBuilder.build();
	}

	public KafkaResult<Long> fetchStoredOffsetsForConsumer(String consumerId, Partition... partitions) {
		FastList<TopicAndPartition> topicsAndPartitions = FastList.newList(Arrays.asList(partitions)).collect(new ConvertToTopicAndPartitionFunction());
		OffsetFetchRequest offsetFetchRequest = new OffsetFetchRequest(consumerId, topicsAndPartitions,
				kafka.api.OffsetFetchRequest.CurrentVersion(), createCorrelationId(), simpleConsumer.clientId());
		OffsetFetchResponse offsetFetchResponse = simpleConsumer.fetchOffsets(offsetFetchRequest);
		KafkaResultBuilder<Long> kafkaResultBuilder = new KafkaResultBuilder<Long>();
		for (Partition partition : partitions) {
			OffsetMetadataAndError offsetMetadataAndError = offsetFetchResponse.offsets().get(partition);
			short errorCode = offsetMetadataAndError.error();
			if (ErrorMapping.NoError() == errorCode) {
				kafkaResultBuilder.add(partition).withResult(offsetMetadataAndError.offset());
			}
			else {
				kafkaResultBuilder.add(partition).withError(errorCode);
			}
		}
		return kafkaResultBuilder.build();
	}

	public KafkaResult<Long> fetchInitialOffset(long referenceTime, Partition... topicsAndPartitions) {
		Assert.isTrue(topicsAndPartitions.length > 0, "Must provide at least one partition");
		Map<TopicAndPartition, PartitionOffsetRequestInfo> infoMap = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		for (Partition partition : topicsAndPartitions) {
			infoMap.put(new TopicAndPartition(partition.getTopic(), partition.getId()), new PartitionOffsetRequestInfo(referenceTime, 1));
		}
		OffsetRequest offsetRequest = new OffsetRequest(infoMap, kafka.api.OffsetRequest.CurrentVersion(), simpleConsumer.clientId());
		OffsetResponse offsetResponse = simpleConsumer.getOffsetsBefore(offsetRequest);
		KafkaResultBuilder<Long> kafkaResultBuilder = new KafkaResultBuilder<Long>();
		for (Partition partition : topicsAndPartitions) {
			short errorCode = offsetResponse.errorCode(partition.getTopic(), partition.getId());
			if (ErrorMapping.NoError() == errorCode) {
				long[] offsets = offsetResponse.offsets(partition.getTopic(), partition.getId());
				if (offsets.length == 0) {
					throw new IllegalStateException("No error has been returned, but no offsets either");
				}
				kafkaResultBuilder.add(partition).withResult(offsets[0]);
			}
			else {
				kafkaResultBuilder.add(partition).withError(errorCode);
			}
		}
		return kafkaResultBuilder.build();
	}

	public KafkaResult<Void> commitOffsetsForConsumer(String consumerId, Map<Partition, Long> offsets) {
		Map<TopicAndPartition, OffsetMetadataAndError> requestInfo =
				MapIterate.collect(offsets, new CreateRequestInfoMapEntryFunction());
		OffsetCommitResponse offsetCommitResponse =
				simpleConsumer.commitOffsets(
						new OffsetCommitRequest(consumerId, requestInfo, kafka.api.OffsetCommitRequest.CurrentVersion(),
								createCorrelationId(), simpleConsumer.clientId()));
		KafkaResultBuilder<Void> kafkaResultBuilder = new KafkaResultBuilder<Void>();
		for (TopicAndPartition topicAndPartition : requestInfo.keySet()) {
			if (offsetCommitResponse.errors().containsKey(topicAndPartition)) {
				Partition partition = new Partition(topicAndPartition.topic(), topicAndPartition.partition());
				kafkaResultBuilder.add(partition).withError((Short) offsetCommitResponse.errors().get(topicAndPartition));
			}
		}
		return kafkaResultBuilder.build();
	}

	public KafkaResult<KafkaBrokerAddress> findLeaders(String... topics) {
		TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Arrays.asList(topics), createCorrelationId());
		TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);
		KafkaResultBuilder<KafkaBrokerAddress> kafkaResultBuilder = new KafkaResultBuilder<KafkaBrokerAddress>();
		for (TopicMetadata topicMetadata : topicMetadataResponse.topicsMetadata()) {
			if (topicMetadata.errorCode() != ErrorMapping.NoError()) {
				kafkaResultBuilder.add(new Partition(topicMetadata.topic(), -1)).withError(topicMetadata.errorCode());
			}
			else {
				for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
					Partition partition = new Partition(topicMetadata.topic(), partitionMetadata.partitionId());
					if (ErrorMapping.NoError() == partitionMetadata.errorCode()) {
						Broker leader = partitionMetadata.leader();
						KafkaBrokerAddress result = new KafkaBrokerAddress(leader.host(), leader.port());
						kafkaResultBuilder.add(partition).withResult(result);
					}
					else {
						kafkaResultBuilder.add(partition).withError(partitionMetadata.errorCode());
					}
				}

			}
		}
		return kafkaResultBuilder.build();
	}

	/**
	 * Creates a pseudo-unique correlation id for requests and responses
	 *
	 * @return correlation id
	 */
	private Integer createCorrelationId() {
		return correlationIdCounter.incrementAndGet();
	}

	private static class ConvertToKafkaMessageFunction implements Function<MessageAndOffset, KafkaMessage> {
		private final KafkaMessageFetchRequest request;

		public ConvertToKafkaMessageFunction(KafkaMessageFetchRequest request) {
			this.request = request;
		}

		@Override
		public KafkaMessage valueOf(MessageAndOffset messageAndOffset) {
			return new KafkaMessage(messageAndOffset.message(),
					new KafkaMessageMetadata(request.getPartition(), messageAndOffset.offset(), messageAndOffset.nextOffset()));
		}
	}

	private static class ConvertToTopicAndPartitionFunction implements Function<Partition, TopicAndPartition> {
		@Override
		public TopicAndPartition valueOf(Partition partition) {
			return new TopicAndPartition(partition.getTopic(), partition.getId());
		}
	}

	private static class CreateRequestInfoMapEntryFunction implements Function2<Partition, Long, Pair<TopicAndPartition, OffsetMetadataAndError>> {
		@Override
		public Pair<TopicAndPartition, OffsetMetadataAndError> value(Partition partition, Long offset) {
			return Tuples.pair(
					new TopicAndPartition(partition.getTopic(),
							partition.getId()), new OffsetMetadataAndError(offset, OffsetMetadataAndError.NoMetadata(), ErrorMapping.NoError()));
		}
	}
}

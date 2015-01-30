/*
 * Copyright 2015 the original author or authors.
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.impl.block.factory.Functions;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.utility.ListIterate;
import kafka.client.ClientUtils$;
import kafka.common.KafkaException;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataResponse;
import scala.collection.JavaConversions;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ConnectionFactory}
 *
 * @author Marius Bogoevici
 */
public class DefaultConnectionFactory implements InitializingBean, ConnectionFactory, DisposableBean {

	private final GetBrokersByPartitionFunction getBrokersByPartitionFunction = new GetBrokersByPartitionFunction();

	private final ConnectionInstantiationFunction connectionInstantiationFunction = new ConnectionInstantiationFunction();

	private final Configuration configuration;

	private final AtomicReference<PartitionBrokerMap> partitionBrokerMapReference = new AtomicReference<PartitionBrokerMap>();

	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	private final UnifiedMap<BrokerAddress, Connection> kafkaBrokersCache = UnifiedMap.newMap();

	public DefaultConnectionFactory(Configuration configuration) {
		this.configuration = configuration;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.configuration, "Kafka configuration cannot be empty");
		this.refreshLeaders(configuration.getDefaultTopic() == null ? Collections.<String>emptyList() :
				Collections.singletonList(configuration.getDefaultTopic()));
	}

	@Override
	public void destroy() throws Exception {
		for (Connection connection : kafkaBrokersCache) {
			connection.close();
		}
	}

	/**
	 * @see ConnectionFactory#getLeaders(Iterable)
	 */
	@Override
	public Map<Partition, BrokerAddress> getLeaders(Iterable<Partition> partitions) {
		return FastList.newList(partitions).toMap(Functions.<Partition>getPassThru(), getBrokersByPartitionFunction);
	}

	/**
	 * @see ConnectionFactory#getLeader(Partition)
	 */
	@Override
	public BrokerAddress getLeader(Partition partition) {
		try {
			lock.readLock().lock();
			return this.getLeaders(Collections.singleton(partition)).get(partition);
		}
		finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * @see ConnectionFactory#connect(BrokerAddress)
	 */
	@Override
	public Connection connect(BrokerAddress brokerAddress) {
		return kafkaBrokersCache.getIfAbsentPutWithKey(brokerAddress, connectionInstantiationFunction);
	}

	/**
	 * @see ConnectionFactory#refreshLeaders(Collection)
	 */
	@Override
	public void refreshLeaders(Collection<String> topics) {
		try {
			lock.writeLock().lock();
			for (Connection connection : kafkaBrokersCache) {
				connection.close();
			}
			String brokerAddressesAsString =
					ListIterate.collect(configuration.getBrokerAddresses(), Functions.getToString())
					.makeString(",");
			TopicMetadataResponse topicMetadataResponse =
					new TopicMetadataResponse(
							ClientUtils$.MODULE$.fetchTopicMetadata(
									JavaConversions.asScalaSet(new HashSet<String>(topics)),
									ClientUtils$.MODULE$.parseBrokerList(brokerAddressesAsString),
									this.configuration.getClientId(), this.configuration.getFetchMetadataTimeout(), 0));
			Map<Partition, BrokerAddress> kafkaBrokerAddressMap = new HashMap<Partition, BrokerAddress>();
			for (TopicMetadata topicMetadata : topicMetadataResponse.topicsMetadata()) {
				for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
					kafkaBrokerAddressMap.put(new Partition(topicMetadata.topic(), partitionMetadata.partitionId()),
							new BrokerAddress(partitionMetadata.leader().host(), partitionMetadata.leader().port()));
				}
			}
			this.partitionBrokerMapReference.set(new PartitionBrokerMap(UnifiedMap.newMap(kafkaBrokerAddressMap)));
		}
		finally {
			lock.writeLock().unlock();
		}
	}

	/**
	 * @see ConnectionFactory#getPartitions(String)
	 */
	@Override
	public Collection<Partition> getPartitions(String topic) {
		if (!getPartitionBrokerMap().getPartitionsByTopic().containsKey(topic)) {
			throw new TopicNotFoundException(topic);
		}
		return getPartitionBrokerMap().getPartitionsByTopic().get(topic).toList();
	}

	private PartitionBrokerMap getPartitionBrokerMap() {
		return partitionBrokerMapReference.get();
	}

	@SuppressWarnings("serial")
	private class ConnectionInstantiationFunction implements Function<BrokerAddress, Connection> {

		@Override
		public Connection valueOf(BrokerAddress brokerAddress) {
			return new DefaultConnection(brokerAddress,
					DefaultConnectionFactory.this.configuration.getClientId(),
					DefaultConnectionFactory.this.configuration.getBufferSize(),
					DefaultConnectionFactory.this.configuration.getSocketTimeout(),
					DefaultConnectionFactory.this.configuration.getMinBytes(),
					DefaultConnectionFactory.this.configuration.getMaxWait());
		}

	}

	@SuppressWarnings("serial")
	private class GetBrokersByPartitionFunction implements Function<Partition, BrokerAddress> {
		@Override
		public BrokerAddress valueOf(Partition partition) {
			return partitionBrokerMapReference.get().getBrokersByPartition().get(partition);
		}

	}

}

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
import kafka.client.ClientUtils$;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataResponse;
import scala.collection.JavaConversions;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ConnectionFactory}
 *
 * @author Marius Bogoevici
 */
public class DefaultConnectionFactory implements InitializingBean, ConnectionFactory {

	private final GetBrokersByPartitionFunction getBrokersByPartitionFunction = new GetBrokersByPartitionFunction();

	private final ConnectionInstantiationFunction connectionInstantiationFunction = new ConnectionInstantiationFunction();

	private final Configuration configuration;

	private final AtomicReference<PartitionBrokerMap> partitionBrokerMapReference = new AtomicReference<PartitionBrokerMap>();

	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	private UnifiedMap<BrokerAddress, Connection> kafkaBrokersCache = UnifiedMap.newMap();

	public DefaultConnectionFactory(Configuration configuration) {
		this.configuration = configuration;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.configuration, "Kafka configuration cannot be empty");
		this.refreshLeaders(configuration.getDefaultTopic()==null? Collections.<String>emptyList() : Collections
				.singletonList
				(configuration
				.getDefaultTopic()));
	}

	/**
	 * Retrieves the leaders for a set of partitions.
	 *
	 * @param partitions
	 * @return the broker associated with the provided topic and partition
	 */
	@Override
	public Map<Partition, BrokerAddress> getLeaders(Iterable<Partition> partitions) {
		return FastList.newList(partitions).toMap(Functions.<Partition>getPassThru(), getBrokersByPartitionFunction);
	}

	/**
	 * Returns the leader for a single partition
	 *
	 * @param partition the partition whose leader is queried
	 *
	 * @return the leader's address
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
	 * Creates a connection to a Kafka broker, caching it internally
	 *
	 * @param brokerAddress a broker address
	 *
	 * @return a working connection
	 */
	@Override
	public Connection createConnection(BrokerAddress brokerAddress) {
		return kafkaBrokersCache.getIfAbsentPutWithKey(brokerAddress, connectionInstantiationFunction);
	}

	/**
	 * Refreshes the broker connections and partition leader map. To be called when the topology changes
	 * are detected (i.e. brokers leave and/or partition leaders change) and that results in fetch errors,
	 * for instance
	 *
	 * @param topics the topics for which to refresh the leaders
	 */
	@Override
	public void refreshLeaders(Collection<String> topics) {
		try {
			lock.writeLock().lock();
			for (Connection connection : kafkaBrokersCache) {
				connection.close();
			}
			TopicMetadataResponse topicMetadataResponse = new TopicMetadataResponse(ClientUtils$.MODULE$.fetchTopicMetadata(
					JavaConversions.asScalaSet(new HashSet<String>(topics)),
					ClientUtils$.MODULE$.parseBrokerList(configuration.getBrokerAddressesAsString()),
					"clientId", 10000, 0));
			Map<Partition, BrokerAddress> kafkaBrokerAddressMap = new HashMap<Partition, BrokerAddress>();
			for (TopicMetadata topicMetadata : topicMetadataResponse.topicsMetadata()) {
				for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
					kafkaBrokerAddressMap.put(new Partition(topicMetadata.topic(), partitionMetadata.partitionId()), new
							BrokerAddress(partitionMetadata.leader().host(), partitionMetadata.leader().port()));
				}
			}

			this.partitionBrokerMapReference.set(new PartitionBrokerMap(UnifiedMap.newMap(kafkaBrokerAddressMap)));

		}
		finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public Collection<Partition> getPartitions() {
		return getPartitionBrokerMap().getBrokersByPartition().keysView().toSet();
	}

	@Override
	public Collection<Partition> getPartitions(String topic) {
		return getPartitionBrokerMap().getPartitionsByTopic().get(topic).toList();
	}

	private PartitionBrokerMap getPartitionBrokerMap() {
		return partitionBrokerMapReference.get();
	}

	@SuppressWarnings("serial")
	private class ConnectionInstantiationFunction implements Function<BrokerAddress, Connection> {
		@Override
		public Connection valueOf(BrokerAddress brokerAddress) {
				return new DefaultConnection(brokerAddress);
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

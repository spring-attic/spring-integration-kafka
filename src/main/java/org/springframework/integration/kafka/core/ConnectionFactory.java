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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
 * Creates Kafka connections and retrieves metadata for topics and partitions
 *
 * @author Marius Bogoevici
 */
public class ConnectionFactory implements InitializingBean {

	private final GetBrokersByPartitionFunction getBrokersByPartitionFunction = new GetBrokersByPartitionFunction();

	private final ConnectionInstantiationFunction connectionInstantiationFunction = new ConnectionInstantiationFunction();

	private final Configuration configuration;

	private final AtomicReference<PartitionBrokerMap> partitionBrokerMapReference = new AtomicReference<PartitionBrokerMap>();

	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	private UnifiedMap<BrokerAddress, Connection> kafkaBrokersCache = UnifiedMap.newMap();

	private Connection defaultConnection;

	public ConnectionFactory(Configuration configuration) {
		this.configuration = configuration;
	}

	public List<BrokerAddress> getBrokerAddresses() {
		return configuration.getBrokerAddresses();
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
	public Map<Partition, BrokerAddress> getLeaders(Iterable<Partition> partitions) {
		return FastList.newList(partitions).toMap(Functions.<Partition>getPassThru(), getBrokersByPartitionFunction);
	}

	/**
	 * Returns the leader for a single partition
	 *
	 * @param partition
	 * @return
	 */
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
	 * Returns the partitions of which the given broker is the leader
	 *
	 * @param brokerAddress
	 * @return
	 */
	public List<Partition> getPartitions(BrokerAddress brokerAddress) {
		return this.partitionBrokerMapReference.get().getPartitionsByBroker().get(brokerAddress).toList();
	}

	/**
	 * Creates a connection to a Kafka broker, caching it internally
	 *
	 * @param brokerAddress
	 * @return
	 */
	public Connection createConnection(BrokerAddress brokerAddress) {
		return kafkaBrokersCache.getIfAbsentPutWithKey(brokerAddress, connectionInstantiationFunction);
	}

	/**
	 * Refreshes the broker connections and partition leader internal. To be called when the topology changes
	 * (i.e. brokers leave and/or partition leaders change)
	 * @param topics
	 */
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

	public Collection<Partition> getPartitions() {
		return getPartitionBrokerMap().getBrokersByPartition().keysView().toSet();
	}

	public Collection<Partition> getPartitions(String topic) {
		return getPartitionBrokerMap().getPartitionsByTopic().get(topic).toList();
	}

	private PartitionBrokerMap getPartitionBrokerMap() {
		return partitionBrokerMapReference.get();
	}

	private class ConnectionInstantiationFunction implements Function<BrokerAddress, Connection> {
		@Override
		public Connection valueOf(BrokerAddress brokerAddress) {
			ConnectionFactory connectionFactory = ConnectionFactory.this;
			if (connectionFactory.defaultConnection != null
					&& connectionFactory.defaultConnection.getBrokerAddress().equals(brokerAddress)) {
				return ConnectionFactory.this.defaultConnection;
			}
			else {
				return new Connection(brokerAddress);
			}
		}
	}

	private class GetBrokersByPartitionFunction implements Function<Partition, BrokerAddress> {
		@Override
		public BrokerAddress valueOf(Partition partition) {
			return ConnectionFactory.this.partitionBrokerMapReference.get().getBrokersByPartition().get(partition);
		}
	}
}

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
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.block.predicate.Predicate;
import com.gs.collections.api.partition.PartitionIterable;
import com.gs.collections.impl.block.factory.Functions;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.utility.Iterate;
import com.gs.collections.impl.utility.ListIterate;
import kafka.client.ClientUtils$;
import kafka.common.ErrorMapping;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import scala.collection.JavaConversions;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ConnectionFactory}
 *
 * @author Marius Bogoevici
 */
public class DefaultConnectionFactory implements InitializingBean, ConnectionFactory, DisposableBean {

	private final static Log log = LogFactory.getLog(DefaultConnectionFactory.class);

	public static final Predicate<TopicMetadata> errorlessTopicMetadataPredicate = new ErrorlessTopicMetadataPredicate();

	private final GetBrokersByPartitionFunction getBrokersByPartitionFunction = new GetBrokersByPartitionFunction();

	private final ConnectionInstantiationFunction connectionInstantiationFunction = new ConnectionInstantiationFunction();

	private final Configuration configuration;

	private final AtomicReference<MetadataCache> metadataCacheHolder =
			new AtomicReference<MetadataCache>(new MetadataCache(Collections.<TopicMetadata>emptySet()));

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
		this.refreshMetadata(configuration.getDefaultTopic() == null ? Collections.<String>emptyList() :
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
		return Iterate.toMap(partitions, Functions.<Partition>getPassThru(), getBrokersByPartitionFunction);
	}

	/**
	 * @see ConnectionFactory#getLeader(Partition)
	 */
	@Override
	public BrokerAddress getLeader(Partition partition) {
		BrokerAddress leader = null;
		try {
			lock.readLock().lock();
			leader = getMetadataCache().getLeader(partition);
		}
		finally {
			lock.readLock().unlock();
		}
		if (leader == null) {
			try {
				lock.writeLock().lock();
				// double lock check
				leader = getMetadataCache().getLeader(partition);
				if (leader == null) {
					this.refreshMetadata(Collections.singleton(partition.getTopic()));
					leader = getMetadataCache().getLeader(partition);
				}
			}
			finally {
				lock.writeLock().unlock();
			}
		}
		if (leader == null) {
			throw new PartitionNotFoundException(partition);
		}
		return leader;
	}

	/**
	 * @see ConnectionFactory#connect(BrokerAddress)
	 */
	@Override
	public Connection connect(BrokerAddress brokerAddress) {
		return kafkaBrokersCache.getIfAbsentPutWithKey(brokerAddress, connectionInstantiationFunction);
	}

	/**
	 * @see ConnectionFactory#refreshMetadata(Collection)
	 */
	@Override
	public void refreshMetadata(Collection<String> topics) {
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
			PartitionIterable<TopicMetadata> selectWithoutErrors = Iterate.partition(topicMetadataResponse.topicsMetadata(),
					errorlessTopicMetadataPredicate);
			this.metadataCacheHolder.set(this.metadataCacheHolder.get().merge(selectWithoutErrors.getSelected()));
			for (TopicMetadata topicMetadata : selectWithoutErrors.getRejected()) {
				log.error(String.format("No metadata could be retrieved for '%s'", topicMetadata.topic()),
						ErrorMapping.exceptionFor(topicMetadata.errorCode()));
			}
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
		// first, we try to read the topic from the cache. We use the read lock to block if a write is in progress
		Collection<Partition> returnedPartitions = null;
		try {
			lock.readLock().lock();
			returnedPartitions = getMetadataCache().getPartitions(topic);
		}
		finally {
			lock.readLock().unlock();
		}
		// if we got here, it means that the data was not available, we should try a refresh. The lock is reentrant
		// so we will not block ourselves
		if (returnedPartitions == null) {
			try {
				lock.writeLock().lock();
				// double lock check
				returnedPartitions = getMetadataCache().getPartitions(topic);
				if (returnedPartitions == null) {
					this.refreshMetadata(Collections.singleton(topic));
					// if data is not available after refreshing, it means that the topic was not found
					returnedPartitions = getMetadataCache().getPartitions(topic);
				}
			}
			finally {
				lock.writeLock().unlock();
			}
		}
		if (returnedPartitions == null) {
			throw new TopicNotFoundException(topic);
		}
		return returnedPartitions;
	}

	private MetadataCache getMetadataCache() {
		return metadataCacheHolder.get();
	}

	@SuppressWarnings("serial")
	private static class ErrorlessTopicMetadataPredicate implements Predicate<TopicMetadata> {
		@Override
		public boolean accept(TopicMetadata topicMetadata) {
			return topicMetadata.errorCode() == ErrorMapping.NoError();
		}
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
			return metadataCacheHolder.get().getLeader(partition);
		}

	}

}

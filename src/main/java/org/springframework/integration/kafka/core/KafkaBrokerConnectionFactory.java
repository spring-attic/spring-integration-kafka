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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.impl.block.factory.Functions;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * Creates Kafka connections and retrieves metadata for topics and partitions
 *
 * @author Marius Bogoevici
 */
public class KafkaBrokerConnectionFactory implements InitializingBean {

	private final KafkaConfiguration kafkaConfiguration;

	private UnifiedMap<KafkaBrokerAddress, KafkaBrokerConnection> kafkaBrokersCache = UnifiedMap.newMap();

	private final AtomicReference<PartitionBrokerMap> partitionBrokerMapReference = new AtomicReference<PartitionBrokerMap>();

	private KafkaBrokerConnection defaultConnection;

	public KafkaBrokerConnectionFactory(KafkaConfiguration kafkaConfiguration) {
		this.kafkaConfiguration = kafkaConfiguration;
	}

	public List<KafkaBrokerAddress> getBrokerAddresses() {
		return kafkaConfiguration.getBrokerAddresses();
	}

	public KafkaConfiguration getKafkaConfiguration() {
		return kafkaConfiguration;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(this.kafkaConfiguration, "Kafka configuration cannot be empty");
		this.refreshLeaders();
	}


	/**
	 * Retrieves the leaders for a set of partitions.
	 *
	 * @param partitions
	 * @return the broker associated with the provided topic and partition
	 */
	public Map<Partition, KafkaBrokerAddress> getLeaders(Partition... partitions) {
		return FastList.newListWith(partitions).toMap(Functions.<Partition>getPassThru(), new Function<Partition, KafkaBrokerAddress>() {
			@Override
			public KafkaBrokerAddress valueOf(Partition partition) {
				return KafkaBrokerConnectionFactory.this.partitionBrokerMapReference.get().getBrokersByPartition().get(partition);
			}
		});
	}

	/**
	 * Returns the leader for a single partition
	 *
	 * @param partition
	 * @return
	 */
	public KafkaBrokerAddress getLeader(Partition partition) {
		return this.getLeaders(partition).get(partition);
	}

	/**
	 * Returns the partitions of which the given broker is the leader
	 *
	 * @param kafkaBrokerAddress
	 * @return
	 */
	public List<Partition> getPartitions(KafkaBrokerAddress kafkaBrokerAddress) {
		return this.partitionBrokerMapReference.get().getPartitionsByBroker().get(kafkaBrokerAddress).toList();
	}

	/**
	 * Creates a connection to a Kafka broker, caching it internally
	 *
	 * @param kafkaBrokerAddress
	 * @return
	 */
	public KafkaBrokerConnection createConnection(KafkaBrokerAddress kafkaBrokerAddress) {
		return kafkaBrokersCache.getIfAbsentPutWith(kafkaBrokerAddress, new KafkaBrokerConnectionInstantiator(), kafkaBrokerAddress);
	}

	/**
	 * Refreshes the broker connections and partition leader internal. To be called when the topology changes (i.e. brokers leave and/or partition leaders change)
	 */
	public void refreshLeaders() {
		synchronized (partitionBrokerMapReference) {
			for (KafkaBrokerConnection kafkaBrokerConnection : kafkaBrokersCache) {
				kafkaBrokerConnection.close();
			}
			Iterator<KafkaBrokerAddress> kafkaBrokerAddressIterator = kafkaConfiguration.getBrokerAddresses().iterator();
			do {
				KafkaBrokerConnection candidateConnection = this.createConnection(kafkaBrokerAddressIterator.next());
				KafkaResult<KafkaBrokerAddress> leaders = candidateConnection.findLeaders();
				if (leaders.getErrors().size() == 0) {
					this.defaultConnection = candidateConnection;
					this.partitionBrokerMapReference.set(new PartitionBrokerMap(UnifiedMap.newMap(leaders.getResults())));
				}
			} while (kafkaBrokerAddressIterator.hasNext() && this.defaultConnection == null);
		}
	}

	public Collection<Partition> getPartitions() {
		return getPartitionBrokerMap().getBrokersByPartition().keysView().toSet();
	}

	public Collection<Partition> getPartitions(String topic) {
		return getPartitionBrokerMap().getPartitionsByTopic().get(topic).toList();
	}

	private class KafkaBrokerConnectionInstantiator implements Function<KafkaBrokerAddress, KafkaBrokerConnection> {
		@Override
		public KafkaBrokerConnection valueOf(KafkaBrokerAddress kafkaBrokerAddress) {
			KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory = KafkaBrokerConnectionFactory.this;
			if (kafkaBrokerConnectionFactory.defaultConnection != null
					&& kafkaBrokerConnectionFactory.defaultConnection.getBrokerAddress().equals(kafkaBrokerAddress)) {
				return KafkaBrokerConnectionFactory.this.defaultConnection;
			}
			else {
				return new KafkaBrokerConnection(kafkaBrokerAddress);
			}
		}
	}

	private PartitionBrokerMap getPartitionBrokerMap() {
		return partitionBrokerMapReference.get();
	}

}

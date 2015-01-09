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

import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.map.ImmutableMap;
import com.gs.collections.api.multimap.Multimap;
import com.gs.collections.impl.map.mutable.UnifiedMap;

/**
 * Immutable store for the partition/broker mapping
 *
 * @author Marius Bogoevici
 */
class PartitionBrokerMap {

	private Multimap<KafkaBrokerAddress, Partition> partitionsByBroker;

	private ImmutableMap<Partition, KafkaBrokerAddress> brokersByPartition;

	private Multimap<String, Partition> partitionsByTopic;

	public PartitionBrokerMap(UnifiedMap<Partition, KafkaBrokerAddress> brokersByPartition) {
		this.brokersByPartition = brokersByPartition.toImmutable();
		this.partitionsByTopic = this.brokersByPartition.keysView().groupBy(new Function<Partition, String>() {
			@Override
			public String valueOf(Partition partition) {
				return partition.getTopic();
			}
		});
		this.partitionsByBroker = brokersByPartition.flip().toImmutable();
	}

	public Multimap<KafkaBrokerAddress, Partition> getPartitionsByBroker() {
		return partitionsByBroker;
	}

	public ImmutableMap<Partition, KafkaBrokerAddress> getBrokersByPartition() {
		return brokersByPartition;
	}

	public Multimap<String, Partition> getPartitionsByTopic() {
		return partitionsByTopic;
	}
}

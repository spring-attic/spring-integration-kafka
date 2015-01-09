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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.gs.collections.api.RichIterable;
import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.block.procedure.Procedure2;
import com.gs.collections.api.map.MutableMap;
import com.gs.collections.api.multimap.MutableMultimap;
import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import com.gs.collections.impl.utility.Iterate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.integration.kafka.core.KafkaBrokerAddress;
import org.springframework.integration.kafka.core.KafkaBrokerConnectionFactory;
import org.springframework.integration.kafka.core.KafkaResult;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.metadata.MetadataStore;
import org.springframework.integration.metadata.SimpleMetadataStore;
import org.springframework.util.CollectionUtils;

/**
 * An {@link org.springframework.integration.kafka.simple.offset.OffsetManager} that persists offsets into
 * a {@link org.springframework.integration.metadata.MetadataStore}
 *
 * @author Marius Bogoevici
 */
public class MetadataStoreOffsetManager implements OffsetManager {

	private final static Log LOG = LogFactory.getLog(MetadataStoreOffsetManager.class);

	private String consumerId = "spring.kafka";

	private MetadataStore metadataStore = new SimpleMetadataStore();

	private KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory;

	private long referenceTimestamp = -2;

	private MutableMap<Partition, Long> offsets = new ConcurrentHashMap<Partition, Long>();

	public MetadataStoreOffsetManager(KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory) {
		this(kafkaBrokerConnectionFactory, null);
	}

	public MetadataStoreOffsetManager(KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory, Map<Partition, Long> initialOffsets) {
		this.kafkaBrokerConnectionFactory = kafkaBrokerConnectionFactory;
		loadOffsets(initialOffsets);
	}

	public String getConsumerId() {
		return consumerId;
	}

	/**
	 * The identifier of a consumer of Kafka messages. Allows to store separate sets of offsets in the {@link MetadataStore}.
	 *
	 * @param consumerId the consumer ID
	 */
	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}

	public MetadataStore getMetadataStore() {
		return metadataStore;
	}

	/**
	 * The backing {@link MetadataStore} for storing offsets.
	 *
	 * @param metadataStore a fully configured {@link MetadataStore} instance
	 */
	public void setMetadataStore(MetadataStore metadataStore) {
		this.metadataStore = metadataStore;
	}

	public long getReferenceTimestamp() {
		return referenceTimestamp;
	}

	/**
	 * A timestamp to be used for resetting initial offsets, if they are not available in the {@link MetadataStore}
	 *
	 * @param referenceTimestamp
	 */
	public void setReferenceTimestamp(long referenceTimestamp) {
		this.referenceTimestamp = referenceTimestamp;
	}

	/**
	 * @see {@link OffsetManager#updateOffset(Partition, long)}
	 */
	@Override
	public void updateOffset(Partition partition, long offset) {
		metadataStore.put(asKey(partition), Long.toString(offset));
		offsets.put(partition, offset);
	}

	/**
	 * @see {@link OffsetManager#getOffset(Partition)}
	 */
	@Override
	public long getOffset(Partition partition) {
		return offsets.get(partition);
	}

	private void loadOffsets(Map<Partition, Long> initialOffsets) {
		if (!CollectionUtils.isEmpty(initialOffsets)) {
			this.offsets = ConcurrentHashMap.newMap(initialOffsets);
		}
		else {
			this.offsets = ConcurrentHashMap.newMap();
		}
		List<Partition> partitionsRequiringInitialOffsets = new ArrayList<Partition>();
		for (Partition partition : kafkaBrokerConnectionFactory.getPartitions()) {
			if (!this.offsets.containsKey(partition)) {
				String storedOffsetValueAsString = this.metadataStore.get(asKey(partition));
				Long storedOffsetValue = null;
				if (storedOffsetValueAsString != null) {
					try {
						storedOffsetValue = Long.parseLong(storedOffsetValueAsString);
					}
					catch (NumberFormatException e) {
						LOG.warn("Invalid value: " + storedOffsetValue);
					}
				}
				if (storedOffsetValue != null) {
					offsets.put(partition, storedOffsetValue);
				}
				else {
					partitionsRequiringInitialOffsets.add(partition);
				}
			}
		}
		if (partitionsRequiringInitialOffsets.size() > 0) {
			MutableMultimap<KafkaBrokerAddress, Partition> partitionsByLeaderAddress = Iterate.groupBy(partitionsRequiringInitialOffsets, new GetLeaderFunction());
			partitionsByLeaderAddress.toMap().forEachKeyValue(new Procedure2<KafkaBrokerAddress, RichIterable<Partition>>() {
				@Override
				public void value(KafkaBrokerAddress kafkaBrokerAddress, RichIterable<Partition> partitions) {
					Partition[] partitionsAsArray = partitions.toArray(new Partition[partitions.size()]);
					KafkaResult<Long> initialOffsets = kafkaBrokerConnectionFactory
							.createConnection(kafkaBrokerAddress).fetchInitialOffset(referenceTimestamp, partitionsAsArray);
					if (initialOffsets.getErrors().size() == 0) {
						for (Partition partition : partitions) {
							offsets.put(partition, initialOffsets.getResults().get(partition));
						}
					}
					else {
						throw new IllegalStateException("Cannot load initial offsets");
					}
				}
			});

		}
	}

	private String asKey(Partition partition) {
		return partition.getTopic() + " " + partition.getId() + " " + consumerId;
	}

	private class GetLeaderFunction implements Function<Partition, KafkaBrokerAddress> {
		@Override
		public KafkaBrokerAddress valueOf(Partition object) {
			return kafkaBrokerConnectionFactory.getLeader(object);
		}
	}
}

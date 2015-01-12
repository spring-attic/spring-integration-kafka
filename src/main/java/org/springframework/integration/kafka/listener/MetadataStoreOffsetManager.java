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


package org.springframework.integration.kafka.listener;

import static com.gs.collections.impl.utility.Iterate.groupBy;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.gs.collections.api.RichIterable;
import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.block.procedure.Procedure2;
import com.gs.collections.api.map.MutableMap;
import com.gs.collections.api.multimap.MutableMultimap;
import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import kafka.api.OffsetRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.integration.kafka.core.BrokerAddress;
import org.springframework.integration.kafka.core.ConnectionFactory;
import org.springframework.integration.kafka.core.KafkaConsumerDefaults;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.core.Result;
import org.springframework.integration.metadata.MetadataStore;
import org.springframework.integration.metadata.SimpleMetadataStore;
import org.springframework.util.CollectionUtils;

/**
 * An {@link OffsetManager} that persists offsets into a {@link MetadataStore}.
 * This offset manager maintains a transient internal state, which is the
 *
 * @author Marius Bogoevici
 */
public class MetadataStoreOffsetManager implements OffsetManager {

	private final static Log log = LogFactory.getLog(MetadataStoreOffsetManager.class);

	private final GetLeaderFunction getLeaderFunction = new GetLeaderFunction();

	private String consumerId = KafkaConsumerDefaults.GROUP_ID;

	private MetadataStore metadataStore = new SimpleMetadataStore();

	private ConnectionFactory connectionFactory;

	private long referenceTimestamp = KafkaConsumerDefaults.DEFAULT_OFFSET_RESET;

	private MutableMap<Partition, Long> offsets = new ConcurrentHashMap<Partition, Long>();

	public MetadataStoreOffsetManager(ConnectionFactory connectionFactory) {
		this(connectionFactory, null);
	}

	public MetadataStoreOffsetManager(ConnectionFactory connectionFactory, Map<Partition, Long> initialOffsets) {
		this.connectionFactory = connectionFactory;
		if (!CollectionUtils.isEmpty(initialOffsets)) {
			this.offsets = ConcurrentHashMap.newMap(initialOffsets);
		}
		else {
			this.offsets = ConcurrentHashMap.newMap();
		}
		if (!CollectionUtils.isEmpty(initialOffsets)) {
			initializeOffsets(initialOffsets.keySet());
		}
	}

	public String getConsumerId() {
		return consumerId;
	}

	/**
	 * The identifier of a consumer of Kafka messages. Allows to store separate sets of offsets in the {@link MetadataStore}.*
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
	 * @see OffsetManager#updateOffset(Partition, long)
	 */
	@Override
	public void updateOffset(Partition partition, long offset) {
		metadataStore.put(asKey(partition), Long.toString(offset));
		offsets.put(partition, offset);
	}

	/**
	 * @see OffsetManager#getOffset(Partition)
	 */
	@Override
	public long getOffset(Partition partition) {
		if (!offsets.containsKey(partition)) {
			initializeOffsets(Collections.singleton(partition));
		}
		if (!offsets.containsKey(partition)) {
			throw new IllegalStateException("Offset " + offsets + " cannot be found");
		}
		return offsets.get(partition);
	}

	@Override
	public void resetOffsets(Collection<Partition> partitionsToReset) {
		MutableMultimap<BrokerAddress, Partition> partitionsByLeaderAddress = groupBy(partitionsToReset, getLeaderFunction);
		partitionsByLeaderAddress.toMap().forEachKeyValue(new RetrieveInitialOffsetsAndUpdateProcedure());
	}

	@Override
	public void close() throws IOException {
		// Flush before closing. This may be redundant, but ensures that the metadata store is closed properly
		flush();
		if (metadataStore instanceof Closeable) {
			((Closeable) metadataStore).close();
		}
	}

	@Override
	public void flush() throws IOException {
		if (metadataStore instanceof Flushable) {
			((Flushable) metadataStore).flush();
		}
	}

	private void initializeOffsets(Collection<Partition> partitions) {
		List<Partition> partitionsRequiringInitialOffsets = new ArrayList<Partition>();
		for (Partition partition : partitions) {
			if (!this.offsets.containsKey(partition)) {
				String storedOffsetValueAsString = this.metadataStore.get(asKey(partition));
				Long storedOffsetValue = null;
				if (storedOffsetValueAsString != null) {
					try {
						storedOffsetValue = Long.parseLong(storedOffsetValueAsString);
					}
					catch (NumberFormatException e) {
						log.warn("Invalid value: " + storedOffsetValue);
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
			resetOffsets(partitionsRequiringInitialOffsets);
		}
	}

	private String asKey(Partition partition) {
		return partition.getTopic() + " " + partition.getId() + " " + consumerId;
	}

	@SuppressWarnings("serial")
	private class GetLeaderFunction implements Function<Partition, BrokerAddress> {
		@Override
		public BrokerAddress valueOf(Partition object) {
			return connectionFactory.getLeader(object);
		}
	}

	@SuppressWarnings("serial")
	private class RetrieveInitialOffsetsAndUpdateProcedure implements Procedure2<BrokerAddress, RichIterable<Partition>> {
		@Override
		public void value(BrokerAddress brokerAddress, RichIterable<Partition> partitions) {
			Partition[] partitionsAsArray = partitions.toArray(new Partition[partitions.size()]);
			Result<Long> initialOffsets = connectionFactory.createConnection(brokerAddress).fetchInitialOffset(referenceTimestamp, partitionsAsArray);
			if (initialOffsets.getErrors().size() == 0) {
				for (Partition partition : partitions) {
					offsets.put(partition, initialOffsets.getResults().get(partition));
				}
			}
			else {
				throw new IllegalStateException("Cannot load initial offsets");
			}
		}
	}
}

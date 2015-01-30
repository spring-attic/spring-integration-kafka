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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import kafka.common.ErrorMapping;

import org.springframework.integration.kafka.core.BrokerAddress;
import org.springframework.integration.kafka.core.Connection;
import org.springframework.integration.kafka.core.ConnectionFactory;
import org.springframework.integration.kafka.core.ConsumerException;
import org.springframework.integration.kafka.core.KafkaConsumerDefaults;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.core.PartitionNotFoundException;
import org.springframework.integration.kafka.core.Result;
import org.springframework.integration.metadata.MetadataStore;

/**
 * Base implementation for {@link OffsetManager}. Subclasses may customize functionality as necessary.
 *
 * @author Marius Bogoevici
 */
public abstract class AbstractOffsetManager implements OffsetManager {

	protected String consumerId = KafkaConsumerDefaults.GROUP_ID;

	protected long referenceTimestamp = KafkaConsumerDefaults.DEFAULT_OFFSET_RESET;

	protected ConnectionFactory connectionFactory;

	protected Map<Partition, Long> initialOffsets;

	public AbstractOffsetManager(ConnectionFactory connectionFactory, Map<Partition, Long> initialOffsets) {
		this.connectionFactory = connectionFactory;
		this.initialOffsets = initialOffsets == null ? new HashMap<Partition, Long>() : initialOffsets;
	}

	public String getConsumerId() {
		return consumerId;
	}

	/**
	 * The identifier of a consumer of Kafka messages. Allows to store separate sets of offsets in the
	 * {@link MetadataStore}.
	 *
	 * @param consumerId the consumer ID
	 */
	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}

	/**
	 * A timestamp to be used for resetting initial offsets, if they are not available in the {@link MetadataStore}
	 * @param referenceTimestamp the reset timestamp for initial offsets
	 */
	public void setReferenceTimestamp(long referenceTimestamp) {
		this.referenceTimestamp = referenceTimestamp;
	}


	/**
	 * @see OffsetManager#updateOffset(Partition, long)
	 */
	@Override
	public synchronized final void updateOffset(Partition partition, long offset) {
		doUpdateOffset(partition, offset);
	}

	/**
	 * @see OffsetManager#getOffset(Partition)
	 */
	@Override
	public synchronized final long getOffset(Partition partition) {
		Long offsetInMetadataStore = doGetOffset(partition);
		if (offsetInMetadataStore == null) {
			if (this.initialOffsets.containsKey(partition)) {
				return this.initialOffsets.get(partition);
			}
			else {
				BrokerAddress leader = this.connectionFactory.getLeader(partition);
				if (leader == null) {
					throw new PartitionNotFoundException(partition);
				}
				Connection connection = this.connectionFactory.connect(leader);
				Result<Long> offsetResult = connection.fetchInitialOffset(referenceTimestamp, partition);
				if (offsetResult.getErrors().size() > 0) {
					throw new ConsumerException(ErrorMapping.exceptionFor(offsetResult.getError(partition)));
				}
				if (!offsetResult.getResults().containsKey(partition)) {
					throw new IllegalStateException("Result does not contain an expected value");
				}
				return offsetResult.getResult(partition);
			}
		}
		else {
			return offsetInMetadataStore;
		}
	}

	@Override
	public synchronized void resetOffsets(Collection<Partition> partitionsToReset) {
		// any information about those offsets is invalid and can be ignored
		for (Partition partition : partitionsToReset) {
			doRemoveOffset(partition);
			initialOffsets.remove(partition);
		}
	}

	@Override
	public synchronized void deleteOffset(Partition partition) {
		doRemoveOffset(partition);
	}

	protected abstract void doUpdateOffset(Partition partition, long offset);

	protected abstract void doRemoveOffset(Partition partition);

	protected abstract Long doGetOffset(Partition partition);
}

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.utility.ArrayIterate;


/**
 * A Template for executing high-level operations on a set of Kafka brokers.
 *
 * @author Marius Bogoevici
 */
public class KafkaTemplate {

	private final KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory;

	public KafkaTemplate(KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory) {
		this.kafkaBrokerConnectionFactory = kafkaBrokerConnectionFactory;
	}

	public KafkaBrokerConnectionFactory getKafkaBrokerConnectionFactory() {
		return kafkaBrokerConnectionFactory;
	}

	public List<KafkaMessageBatch> receive(KafkaBrokerAddress kafkaBrokerAddress, final Map<Partition, Long> offsets, final int maxSize) {
		return this.receive(FastList.newList(kafkaBrokerConnectionFactory.getPartitions(kafkaBrokerAddress)).collect(new Function<Partition, KafkaMessageFetchRequest>() {
			@Override
			public KafkaMessageFetchRequest valueOf(Partition partition) {
				return new KafkaMessageFetchRequest(partition, offsets.get(partition), maxSize);
			}
		}).toTypedArray(KafkaMessageFetchRequest.class));
	}

	public List<KafkaMessageBatch> receive(KafkaMessageFetchRequest... messageFetchRequests) {
		MutableList<KafkaBrokerAddress> distinctBrokerAddresses = ArrayIterate.collect(messageFetchRequests, new Function<KafkaMessageFetchRequest, KafkaBrokerAddress>() {
			@Override
			public KafkaBrokerAddress valueOf(KafkaMessageFetchRequest fetchRequest) {
				return kafkaBrokerConnectionFactory.getLeader(fetchRequest.getPartition());
			}
		}).distinct();
		if (distinctBrokerAddresses.size() != 1) {
			throw new IllegalArgumentException("All messages must be fetched from the same broker");
		}
		KafkaResult<KafkaMessageBatch> fetch = kafkaBrokerConnectionFactory.createConnection(distinctBrokerAddresses.get(0)).fetch(messageFetchRequests);
		if (fetch.getErrors().size() > 0) {
			// synchronously refresh on error
			kafkaBrokerConnectionFactory.refreshLeaders();
		}
		return new ArrayList<KafkaMessageBatch>(fetch.getResults().values());
	}

}

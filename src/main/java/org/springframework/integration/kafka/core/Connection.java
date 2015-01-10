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

import java.util.Map;

/**
 * A connection to a Kafka broker.
 *
 * @author Marius Bogoevici
 */
public interface Connection {

	Result<KafkaMessageBatch> fetch(FetchRequest... requests) throws ConsumerException;

	Result<Long> fetchStoredOffsetsForConsumer(String consumerId, Partition... partitions) throws
			ConsumerException;

	Result<Long> fetchInitialOffset(long referenceTime, Partition... topicsAndPartitions) throws
					ConsumerException;

	Result<Void> commitOffsetsForConsumer(String consumerId, Map<Partition, Long> offsets) throws ConsumerException;

	Result<BrokerAddress> findLeaders(String... topics) throws ConsumerException;

	BrokerAddress getBrokerAddress();

	void close();
}

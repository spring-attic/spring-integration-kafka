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

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Listener for handling incoming Kafka messages, propagating an acknowledgment handle that recipients
 * can invoke when the message has been processed.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @since 1.0.1
 */
public interface AcknowledgingMessageListener<K, V> {

	/**
	 * Executes when a Kafka message is received
	 *
	 * @param record the Kafka message to be processed
	 * @param acknowledgment a handle for acknowledging the message processing
	 */
	void onMessage(ConsumerRecord<K, V> record, Acknowledgment acknowledgment);

}

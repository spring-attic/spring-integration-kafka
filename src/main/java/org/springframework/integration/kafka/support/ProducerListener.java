/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.kafka.support;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Listener for handling outbound Kafka messages. Exactly one of its methods will be invoked, depending on whether
 * the write has been acknowledged or not.
 *
 * Its main goal is to provide a stateless singleton delegate for {@link org.apache.kafka.clients.producer.Callback}s,
 * which, in all but the most trivial cases, requires creating a separate instance per message.
 *
 * @see org.apache.kafka.clients.producer.Callback
 * @author Marius Bogoevici
 * @since 1.3
 */
public interface ProducerListener {

	/**
	 * Invoked after the successful send of a message (that is, after it has been acknowledged by the broker)
	 *
	 * @param topic the destination topic
	 * @param partition the destination partition (could be null)
	 * @param key the key of the outbound message
	 * @param value the payload of the outbound message
	 * @param recordMetadata the result of the successful send operation
	 */
	void onSuccess(String topic, Integer partition, Object key, Object value, RecordMetadata recordMetadata);

	/**
	 * Invoked after an attempt to send a message has failed
	 *
	 * @param topic the destination topic
	 * @param partition the destination partition (could be null)
	 * @param key the key of the outbound message
	 * @param value the payload of the outbound message
	 * @param exception the exception thrown
	 */
	void onError(String topic, Integer partition, Object key, Object value,Exception exception);
}

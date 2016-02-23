/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.kafka.core;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


/**
 * A template for executing high-level operations.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public class KafkaTemplate<K, V> implements KafkaOperations<K, V> {

	protected final Log logger = LogFactory.getLog(this.getClass());

	private final KafkaProducer<K, V> producer;

	private volatile String defaultTopic;

	public KafkaTemplate(Properties configs) {
		this.producer = new KafkaProducer<>(configs);
	}

	public String getDefaultTopic() {
		return defaultTopic;
	}

	public void setDefaultTopic(String defaultTopic) {
		this.defaultTopic = defaultTopic;
	}

	@Override
	public Future<RecordMetadata>  convertAndSend(V data) {
		return convertAndSend(this.defaultTopic, data);
	}

	@Override
	public Future<RecordMetadata>  convertAndSend(K key, V data) {
		return convertAndSend(this.defaultTopic, key, data);
	}

	@Override
	public Future<RecordMetadata>  convertAndSend(int partition, K key, V data) {
		return convertAndSend(this.defaultTopic, partition, key, data);
	}

	@Override
	public Future<RecordMetadata>  convertAndSend(String topic, V data) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, data);
		return doSend(producerRecord);
	}

	@Override
	public Future<RecordMetadata>  convertAndSend(String topic, K key, V data) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, key, data);
		return doSend(producerRecord);
	}

	@Override
	public Future<RecordMetadata>  convertAndSend(String topic, int partition, K key, V data) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, key, data);
		return doSend(producerRecord);
	}

	protected Future<RecordMetadata> doSend(ProducerRecord<K, V> producerRecord) {
		if (logger.isTraceEnabled()) {
			logger.trace("Sending: " + producerRecord);
		}
		Future<RecordMetadata> future = this.producer.send(producerRecord);
		if (logger.isTraceEnabled()) {
			logger.trace("Sent: " + producerRecord);
		}
		return future;
	}

	@Override
	public void flush() {
		this.producer.flush();
	}

}

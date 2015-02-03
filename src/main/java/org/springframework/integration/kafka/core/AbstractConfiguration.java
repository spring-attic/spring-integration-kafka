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

import java.util.List;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Default implementation of a {@link Configuration}, storing the default topic and partitions,
 * as well as connectivity parameters.
 *
 * Implementors must provide a strategy for retrieving the seed brokers.
 *
 * @author Marius Bogoevici
 */
public abstract class AbstractConfiguration implements InitializingBean, Configuration {

	private List<Partition> defaultPartitions;

	private String defaultTopic;

	private String clientId = KafkaConsumerDefaults.GROUP_ID;

	private int minBytes = KafkaConsumerDefaults.MIN_FETCH_BYTES;

	private int maxWait = KafkaConsumerDefaults.MAX_WAIT_TIME_IN_MS;

	private int bufferSize = KafkaConsumerDefaults.SOCKET_BUFFER_SIZE_INT;

	private int socketTimeout = KafkaConsumerDefaults.SOCKET_TIMEOUT_INT;

	private int fetchMetadataTimeout = KafkaConsumerDefaults.FETCH_METADATA_TIMEOUT;


	/**
	 * The minimum amount of data that a server fetch operation will wait for before returning,
	 * unless {@code maxWait} has elapsed.
	 * In conjunction with {@link Configuration#getMaxWait()}}, controls latency
	 * and throughput.
	 * Smaller values increase responsiveness, but may increase the number of poll operations,
	 * potentially reducing throughput and increasing CPU consumption.
	 * @param minBytes the amount of data to fetch
	 */
	public void setMinBytes(int minBytes) {
		this.minBytes = minBytes;
	}

	@Override
	public int getMinBytes() {
		return minBytes;
	}

	/**
	 * The maximum amount of time that a server fetch operation will wait before returning
	 * (unless {@code minFetchSizeInBytes}) are available.
	 * In conjunction with {@link AbstractConfiguration#setMinBytes(int)},
	 * controls latency and throughput.
	 * Smaller intervals increase responsiveness, but may increase
	 * the number of poll operations, potentially increasing CPU
	 * consumption and reducing throughput.
	 * @param maxWait timeout to wait
	 */
	public void setMaxWait(int maxWait) {
		this.maxWait = maxWait;
	}

	@Override
	public int getMaxWait() {
		return maxWait;
	}

	@Override
	public String getClientId() {
		return clientId;
	}

	/**
	 * A client name to be used throughout this connection.
	 * @param clientId the client name
	 */
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	@Override
	public int getBufferSize() {
		return bufferSize;
	}

	/**
	 * The buffer size for this client
	 * @param bufferSize the buffer size
	 */
	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	@Override
	public int getSocketTimeout() {
		return socketTimeout;
	}

	/**
	 * The socket timeout for this client
	 * @param socketTimeout the socket timeout
	 */
	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}


	/**
	 * The timeout on fetching metadata (e.g. partition leaders)
	 * @param fetchMetadataTimeout timeout
	 */
	public void setFetchMetadataTimeout(int fetchMetadataTimeout) {
		this.fetchMetadataTimeout = fetchMetadataTimeout;
	}

	@Override
	public int getFetchMetadataTimeout() {
		return fetchMetadataTimeout;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.isTrue(CollectionUtils.isEmpty(defaultPartitions) || StringUtils.isEmpty(defaultTopic)
				, "A list of default partitions or a default topic may be specified, but not both");
	}

	@Override
	public final List<BrokerAddress> getBrokerAddresses() {
		return doGetBrokerAddresses();
	}

	protected abstract List<BrokerAddress> doGetBrokerAddresses();

	@Override
	public List<Partition> getDefaultPartitions() {
		return defaultPartitions;
	}

	public void setDefaultPartitions(List<Partition> defaultPartitions) {
		this.defaultPartitions = defaultPartitions;
	}

	@Override
	public String getDefaultTopic() {
		return defaultTopic;
	}

	public void setDefaultTopic(String defaultTopic) {
		this.defaultTopic = defaultTopic;
	}


}

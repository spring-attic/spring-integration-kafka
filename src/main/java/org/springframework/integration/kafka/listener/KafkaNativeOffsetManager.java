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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.integration.kafka.core.BrokerAddress;
import org.springframework.integration.kafka.core.Connection;
import org.springframework.integration.kafka.core.ConsumerException;
import org.springframework.integration.kafka.core.DefaultConnectionFactory;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.core.PartitionNotFoundException;
import org.springframework.integration.kafka.core.Result;
import org.springframework.integration.kafka.core.ZookeeperConfiguration;
import org.springframework.integration.kafka.support.ZookeeperConnect;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.listener.RetryListenerSupport;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.gs.collections.impl.factory.Maps;

import kafka.client.ClientUtils$;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.network.BlockingChannel;
import kafka.utils.ZKStringSerializer$;

/**
 * Implementation of an {@link OffsetManager} that uses kafka native 'topic' offset storage.
 *
 * @author Chris Lemper
 */
public class KafkaNativeOffsetManager extends AbstractOffsetManager implements InitializingBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaNativeOffsetManager.class);

	private static final String PARTITION_ATTRIBUTE = "partition";

	private final ZookeeperConnect zookeeperConnect;

	private final Map<Partition, BrokerAddress> offsetManagerBrokerAddressCache;

	private final RetryTemplate retryTemplate;

	/**
	 * @param zookeeperConnect
	 *            the zookeeper connection information
	 * @param initialOffsets
	 *            a map of paritions to initial offsets
	 */
	public KafkaNativeOffsetManager(ZookeeperConnect zookeeperConnect, Map<Partition, Long> initialOffsets) {
		super(new DefaultConnectionFactory(new ZookeeperConfiguration(zookeeperConnect)), initialOffsets);

		this.zookeeperConnect = zookeeperConnect;
		this.offsetManagerBrokerAddressCache = new ConcurrentHashMap<>();
		this.retryTemplate = new RetryTemplate();

		retryTemplate.registerListener(new ResetOffsetManagerBrokerAddressRetryListener());
		final SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
		retryPolicy.setMaxAttempts(5);
		retryTemplate.setRetryPolicy(retryPolicy);
		final ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(125L);
		backOffPolicy.setMaxInterval(5000L);
		backOffPolicy.setMultiplier(2);
		retryTemplate.setBackOffPolicy(backOffPolicy);
	}

	/**
	 * @param zookeeperConnect
	 *            the zookeeper connection information
	 * @param consumerId
	 *            the kafka consumer ID
	 * @param referenceTimestamp
	 *            the reset timestamp for initial offsets
	 */
	public KafkaNativeOffsetManager(ZookeeperConnect zookeeperConnect, String consumerId, long referenceTimestamp) {
		this(zookeeperConnect, Collections.<Partition, Long> emptyMap());
		setConsumerId(consumerId);
		setReferenceTimestamp(referenceTimestamp);
	}

	@Override
	protected Long doGetOffset(final Partition partition) {
		final Long offset = retryTemplate.execute(new RetryCallback<Long, RuntimeException>() {

			@Override
			public Long doWithRetry(RetryContext context) throws RuntimeException {
				context.setAttribute(PARTITION_ATTRIBUTE, partition);
				Result<Long> result = getOffsetManagerConnection(partition).fetchStoredOffsetsForConsumer(
						getConsumerId(), partition);
				checkResultForErrors(result, partition);
				return result.getResult(partition);
			}

			@Override
			public String toString() {
				return String.format("fetchStoredOffsetsForConsumer(%s, %s)", getConsumerId(), partition);
			}
		});

		if (offset != null && offset < 0) {
			return null;
		}

		return offset;
	}

	@Override
	protected void doUpdateOffset(final Partition partition, final long offset) {
		retryTemplate.execute(new RetryCallback<Void, RuntimeException>() {

			@Override
			public Void doWithRetry(RetryContext context) throws RuntimeException {
				context.setAttribute(PARTITION_ATTRIBUTE, partition);
				Result<Void> result = getOffsetManagerConnection(partition).commitOffsetsForConsumer(
						getConsumerId(), Maps.immutable.of(partition, offset).castToMap());
				checkResultForErrors(result, partition);
				return null;
			}

			@Override
			public String toString() {
				return String.format("commitOffsetsForConsumer(%s, %s, %s)", getConsumerId(), partition, offset);
			}
		});
	}

	@Override
	protected void doRemoveOffset(Partition partition) {
		doUpdateOffset(partition, OffsetAndMetadata.InvalidOffset());
	}

	@Override
	public void flush() throws IOException {
		// this function left intentionally blank
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		((DefaultConnectionFactory) connectionFactory).afterPropertiesSet();
	}

	@Override
	public void close() throws IOException {
		try {
			((DefaultConnectionFactory) this.connectionFactory).destroy();
		}
		catch (Exception e) {
			throw new IOException(e);
		}
	}

	private class ResetOffsetManagerBrokerAddressRetryListener extends RetryListenerSupport {

		@Override
		public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable t) {
			if (LOGGER.isWarnEnabled()) {
				LOGGER.warn("Retrying kafka operation [" + callback + "] due to [" + t + "]", t);
			}
			final Object partition = context.getAttribute(PARTITION_ATTRIBUTE);
			if (partition != null) {
				offsetManagerBrokerAddressCache.remove(partition);
			}
		}
	}

	private BrokerAddress getOffsetManagerBrokerAddress(final Partition partition) {
		BrokerAddress brokerAddress = offsetManagerBrokerAddressCache.get(partition);
		if (brokerAddress == null) {
			final String zkServers = zookeeperConnect.getZkConnect();
			final int zkSessionTimeout = 6000;
			final int zkConnectionTimeout = 6000;
			final ZkClient zkClient = new ZkClient(zkServers, zkSessionTimeout, zkConnectionTimeout,
			        ZKStringSerializer$.MODULE$);
			try {
				final int socketTimeoutMs = 3000;
				final int retryBackOffMs = 1000;
				final BlockingChannel channel = ClientUtils$.MODULE$.channelToOffsetManager(getConsumerId(), zkClient,
				        socketTimeoutMs, retryBackOffMs);
				brokerAddress = new BrokerAddress(channel.host(), channel.port());
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Offset manager for [{}] is at [{}].", partition, brokerAddress);
				}
				offsetManagerBrokerAddressCache.put(partition, brokerAddress);
				channel.disconnect();
			}
			finally {
				zkClient.close();
			}
		}
		return brokerAddress;
	}

	private Connection getOffsetManagerConnection(final Partition partition) {
		return connectionFactory.connect(getOffsetManagerBrokerAddress(partition));
	}

	private void checkResultForErrors(final Result<?> result, final Partition partition) {
		if (result.getErrors().containsKey(partition)) {
			final short errorCode = result.getError(partition);
			if (errorCode == ErrorMapping.UnknownTopicOrPartitionCode()) {
				throw new PartitionNotFoundException(partition);
			}
			else if (errorCode != ErrorMapping.NoError()) {
				throw new ConsumerException(ErrorMapping.exceptionFor(errorCode));
			}
		}
	}

}

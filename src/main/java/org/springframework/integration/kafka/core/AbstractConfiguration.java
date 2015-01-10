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

import static com.gs.collections.impl.utility.ListIterate.collect;

import java.util.List;

import com.gs.collections.api.block.function.Function;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Default implementation of a {@link Configuration}, storing the default topic and partitions explicitly.
 *
 * Implementors must provide a strategy for retrieving the seed brokers.
 *
 * @author Marius Bogoevici
 */
public abstract class AbstractConfiguration implements InitializingBean, Configuration {

	public static final BrokerAddressToStringFunction brokerAddressToStringFunction = new BrokerAddressToStringFunction();

	private List<Partition> defaultPartitions;

	private String defaultTopic;

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

	@Override
	public String getBrokerAddressesAsString() {
		return collect(getBrokerAddresses(), brokerAddressToStringFunction).makeString(",");
	}

	@SuppressWarnings("serial")
	private static class BrokerAddressToStringFunction implements Function<BrokerAddress, String> {
		@Override
		public String valueOf(BrokerAddress object) {
			return object.getHost() + ":" + object.getPort();
		}
	}
}

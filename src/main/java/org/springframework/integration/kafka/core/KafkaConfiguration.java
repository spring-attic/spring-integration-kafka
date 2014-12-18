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

import java.util.List;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * Provides a starting configuration for
 * @author Marius Bogoevici
 */
public class KafkaConfiguration implements InitializingBean {

	private List<KafkaBrokerAddress> brokerAddresses;

	private List<Partition> defaultPartitions;

	private String defaultTopic;

	public KafkaConfiguration(List<KafkaBrokerAddress> brokerAddresses) {
		this.brokerAddresses = brokerAddresses;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.isTrue(CollectionUtils.isEmpty(defaultPartitions) || StringUtils.isEmpty(defaultTopic), "A list of default partitions or a default topic may be specified, but not both");
	}

	public List<KafkaBrokerAddress> getBrokerAddresses() {
		return brokerAddresses;
	}

	public void setBrokerAddresses(List<KafkaBrokerAddress> brokerAddresses) {
		this.brokerAddresses = brokerAddresses;
	}

	public void setDefaultPartitions(List<Partition> defaultPartitions) {
		this.defaultPartitions = defaultPartitions;
	}

	public List<Partition> getDefaultPartitions() {
		return defaultPartitions;
	}

	public String getDefaultTopic() {
		return defaultTopic;
	}

	public void setDefaultTopic(String defaultTopic) {
		this.defaultTopic = defaultTopic;
	}
}

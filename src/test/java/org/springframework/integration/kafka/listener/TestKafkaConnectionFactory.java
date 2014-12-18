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


package org.springframework.integration.kafka.listener;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import org.springframework.integration.kafka.core.KafkaBrokerAddress;
import org.springframework.integration.kafka.core.KafkaBrokerConnection;
import org.springframework.integration.kafka.core.KafkaBrokerConnectionFactory;
import org.springframework.integration.kafka.core.KafkaConfiguration;
import org.springframework.integration.kafka.core.KafkaResult;
import org.springframework.integration.kafka.core.Partition;

/**
 * @author Marius Bogoevici
 */
public class TestKafkaConnectionFactory extends AbstractBrokerTest {

	@Rule
	public KafkaEmbeddedBrokerRule kafkaEmbeddedBrokerRule = new KafkaEmbeddedBrokerRule(1);

	@Override
	public KafkaEmbeddedBrokerRule getKafkaRule() {
		return kafkaEmbeddedBrokerRule;
	}

	@Test
	public void testCreateConnectionFactory() throws Exception {

		createTopic(TEST_TOPIC, 1, 1, 1);

		List<KafkaBrokerAddress> brokerAddresses = getKafkaRule().getBrokerAddresses();
		Partition partition = new Partition(TEST_TOPIC, 0);
		KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory = new KafkaBrokerConnectionFactory(new KafkaConfiguration(brokerAddresses));
		kafkaBrokerConnectionFactory.afterPropertiesSet();
		KafkaBrokerConnection connection = kafkaBrokerConnectionFactory.createConnection(getKafkaRule().getBrokerAddresses().get(0));
		KafkaResult<KafkaBrokerAddress> leaders = connection.findLeaders(TEST_TOPIC);
		assertThat(leaders.getErrors().entrySet(), empty());
		assertThat(leaders.getResults().entrySet(), hasSize(1));
		assertThat(leaders.getResults().get(partition), equalTo(getKafkaRule().getBrokerAddresses().get(0)));
	}
}

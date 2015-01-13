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

import static org.hamcrest.Matchers.notNullValue;

import com.gs.collections.api.map.MutableMap;
import com.gs.collections.impl.factory.Maps;
import kafka.message.NoCompressionCodec$;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.kafka.rule.KafkaEmbedded;
import org.springframework.integration.kafka.rule.KafkaRule;
import org.springframework.messaging.Message;

/**
 * @author Marius Bogoevici
 */
public class TestChannelAdapterWithXmlConfiguration extends AbstractMessageListenerContainerTest {

	@Rule
	public final KafkaRule kafkaEmbeddedBrokerRule = new KafkaEmbedded(1);

	@Override
	public KafkaRule getKafkaRule() {
		return this.kafkaEmbeddedBrokerRule;
	}

	@Test
	public void testConsumptionWithXmlConfiguration() throws Exception {

		createTopic(TEST_TOPIC,1,1,1);

		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[]{"classpath:kafka-integration-no-namespace.xml"}, false);

		createStringProducer(NoCompressionCodec$.MODULE$.codec()).send(createMessages(100, TEST_TOPIC));

		MutableMap<String, Object> testProperties = Maps.mutable
				.with("kafka.test.port", (Object) Integer.toString(kafkaEmbeddedBrokerRule.getBrokerAddresses().get(0).getPort()))
				.withKeyValue("kafka.test.topic", TEST_TOPIC);
		context.getEnvironment().getPropertySources()
						.addFirst(new MapPropertySource("test", testProperties));

		context.refresh();

		QueueChannel output = context.getBean("output", QueueChannel.class);

		for (int i = 0; i < 100; i++) {
			Message<?> received = output.receive(1000);
			Assert.assertThat(received, notNullValue());
		}
	}
}

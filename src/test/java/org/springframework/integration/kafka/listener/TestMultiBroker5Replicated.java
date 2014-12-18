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

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Marius Bogoevici
 */
public class TestMultiBroker5Replicated extends AbstractMessageListenerContainerTest {

	@Rule
	public KafkaEmbeddedBrokerRule kafkaEmbeddedBrokerRule = new KafkaEmbeddedBrokerRule(5);

	@Override
	public KafkaEmbeddedBrokerRule getKafkaRule() {
		return kafkaEmbeddedBrokerRule;
	}

	@Test
	public void testLowVolumeLowConcurrency() throws Exception {
		createTopic(TEST_TOPIC, 5, 5, 3);
		runMessageListenerTest(100, 2, 5, 100, 1, 0);
	}

	@Test
	public void testMediumVolumeLowConcurrency() throws Exception {
		createTopic(TEST_TOPIC, 5, 5, 3);
		runMessageListenerTest(100, 2, 5, 1000, 1, 0);
	}

	@Test @Ignore
	public void testHighVolumeLowConcurrency() throws Exception {
		createTopic(TEST_TOPIC, 5, 5, 3);
		runMessageListenerTest(100, 2, 5, 10000, 1, 0);
	}

	@Test
	public void testLowVolumeMediumConcurrency() throws Exception {
		createTopic(TEST_TOPIC, 5, 5, 3);
		runMessageListenerTest(100, 5, 5, 100, 1, 0);
	}

	@Test
	public void testMediumVolumeMediumConcurrency() throws Exception {
		createTopic(TEST_TOPIC, 5, 5, 3);
		runMessageListenerTest(100, 5, 5, 1000, 1, 0);
	}

	@Test @Ignore
	public void testHighVolumeMediumConcurrency() throws Exception {
		createTopic(TEST_TOPIC, 5, 5, 1);
		runMessageListenerTest(100, 5, 5, 100000, 1, 0);
	}


	@Test
	public void testLowVolumeHighConcurrency() throws Exception {
		createTopic(TEST_TOPIC, 100, 5, 3);
		runMessageListenerTest(100, 20, 100, 1000, 1, 0);
	}

	@Test
	public void testMediumVolumeHighConcurrency() throws Exception {
		createTopic(TEST_TOPIC, 100, 5, 3);
		runMessageListenerTest(100, 20, 100, 10000, 1, 0);
	}

	@Test @Ignore
	public void testHighVolumeHighConcurrency() throws Exception {
		createTopic(TEST_TOPIC, 100, 5, 3);
		runMessageListenerTest(100, 20, 100, 100000, 1, 0);
	}


}

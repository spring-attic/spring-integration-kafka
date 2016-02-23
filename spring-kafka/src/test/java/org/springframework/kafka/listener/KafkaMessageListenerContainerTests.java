/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.kafka.listener;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer.ContainerOffsetResetStrategy;
import org.springframework.kafka.rule.KafkaEmbedded;

/**
 * @author Gary Russell
 * @since 2.0
 *
 */
public class KafkaMessageListenerContainerTests {

	private final Log logger = LogFactory.getLog(this.getClass());

	private static String topic1 = "testTopic1";

	private static String topic2 = "testTopic2";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, topic1, topic2);

	@Test
	public void testAutoCommit() throws Exception {
		logger.info("Start auto");
		Properties props = consumerProps("test1", "true");
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(props, topic1);
		final CountDownLatch latch = new CountDownLatch(4);
		container.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("auto: " + message);
				latch.countDown();
			}
		});
		container.setConcurrency(2);
		container.start();
		Thread.sleep(1000);
		Properties senderProps = senderProps();
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(senderProps);
		template.setDefaultTopic("testTopic1");
		template.convertAndSend(0, "foo");
		template.convertAndSend(2, "bar");
		template.convertAndSend(0, "baz");
		template.convertAndSend(2, "qux");
		template.flush();
		assertTrue(latch.await(60, TimeUnit.SECONDS));
		container.stop();
		logger.info("Stop auto");

	}

	@Test
	public void testAfterListenCommit() throws Exception {
		logger.info("Start manual");
		Properties props = consumerProps("test2", "false");
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(props, topic2);
		final CountDownLatch latch = new CountDownLatch(4);
		container.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("manual: " + message);
				latch.countDown();
			}
		});
		container.setConcurrency(2);
		container.start();
		Thread.sleep(1000);
		Properties senderProps = senderProps();
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(senderProps);
		template.setDefaultTopic(topic2);
		template.convertAndSend(0, "foo");
		template.convertAndSend(2, "bar");
		template.convertAndSend(0, "baz");
		template.convertAndSend(2, "qux");
		template.flush();
		assertTrue(latch.await(60, TimeUnit.SECONDS));
		container.stop();
		logger.info("Stop manual");
	}

	@Test
	public void testDefinedPartitions() throws Exception {
		logger.info("Start auto parts");
		Properties props = consumerProps("test1", "true");
		TopicPartition topic1Partition0 = new TopicPartition(topic1, 0);
		KafkaMessageListenerContainer<Integer, String> container1 =
				new KafkaMessageListenerContainer<>(props, topic1Partition0);
		final CountDownLatch latch1 = new CountDownLatch(2);
		container1.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("auto part: " + message);
				latch1.countDown();
			}
		});
		container1.setBeanName("b1");
		container1.start();
		TopicPartition topic1Partition1 = new TopicPartition(topic1, 1);
		KafkaMessageListenerContainer<Integer, String> container2 =
				new KafkaMessageListenerContainer<>(props, topic1Partition1);
		final CountDownLatch latch2 = new CountDownLatch(2);
		container2.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("auto part: " + message);
				latch2.countDown();
			}
		});
		container1.setBeanName("b2");
		container2.start();
		Thread.sleep(1000);
		Properties senderProps = senderProps();
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(senderProps);
		template.setDefaultTopic("testTopic1");
		template.convertAndSend(0, "foo");
		template.convertAndSend(2, "bar");
		template.convertAndSend(0, "baz");
		template.convertAndSend(2, "qux");
		template.flush();
		assertTrue(latch1.await(60, TimeUnit.SECONDS));
		container1.stop();
		container2.stop();

		// reset earliest
		KafkaMessageListenerContainer<Integer, String> resettingContainer = new KafkaMessageListenerContainer<>(props,
				topic1Partition0, topic1Partition1);
		resettingContainer.setBeanName("b3");
		final CountDownLatch latch3 = new CountDownLatch(4);
		resettingContainer.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("auto part e: " + message);
				latch3.countDown();
			}
		});
		resettingContainer.setResetStrategy(ContainerOffsetResetStrategy.EARLIEST);
		resettingContainer.start();
		assertTrue(latch3.await(60, TimeUnit.SECONDS));
		resettingContainer.stop();
		assertThat(latch3.getCount(), equalTo(0L));

		// reset minusone
		resettingContainer = new KafkaMessageListenerContainer<>(props, topic1Partition0, topic1Partition1);
		resettingContainer.setBeanName("b4");
		final CountDownLatch latch4 = new CountDownLatch(2);
		final AtomicReference<String> receivedMessage = new AtomicReference<>();
		resettingContainer.setMessageListener(new MessageListener<Integer, String>() {

			@Override
			public void onMessage(ConsumerRecord<Integer, String> message) {
				logger.info("auto part -1: " + message);
				receivedMessage.set(message.value());
				latch4.countDown();
			}
		});
		resettingContainer.setResetStrategy(ContainerOffsetResetStrategy.RECENT);
		resettingContainer.start();
		assertTrue(latch4.await(60, TimeUnit.SECONDS));
		resettingContainer.stop();
		assertThat(receivedMessage.get(), anyOf(equalTo("baz"), equalTo("qux")));
		assertThat(latch4.getCount(), equalTo(0L));

		logger.info("Stop auto parts");
	}

	private Properties consumerProps(String group, String autoCommit) {
		Properties props = new Properties();
		props.put("bootstrap.servers", embeddedKafka.getBrokersAsString());
//		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", group);
		props.put("enable.auto.commit", autoCommit);
		props.put("auto.commit.interval.ms", "100");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}

	private Properties senderProps() {
		Properties props = new Properties();
		props.put("bootstrap.servers", embeddedKafka.getBrokersAsString());
//		props.put("bootstrap.servers", "localhost:9092");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

}

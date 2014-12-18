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

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.springframework.integration.kafka.inbound.KafkaInboundChannelAdapter.KAFKA_MESSAGE_KEY;
import static org.springframework.integration.kafka.inbound.KafkaInboundChannelAdapter.KAFKA_MESSAGE_OFFSET;
import static org.springframework.integration.kafka.inbound.KafkaInboundChannelAdapter.KAFKA_MESSAGE_PARTITION;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.gs.collections.api.multimap.list.MutableListMultimap;
import com.gs.collections.impl.multimap.list.SynchronizedPutFastListMultimap;
import kafka.message.NoCompressionCodec$;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.integration.kafka.core.ConnectionFactory;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.inbound.KafkaInboundChannelAdapter;
import org.springframework.integration.kafka.serializer.common.StringDecoder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

/**
 * @author Marius Bogoevici
 */
public class TestKafkaInboundChannelAdapter extends AbstractMessageListenerContainerTest {

	@Rule
	public final KafkaEmbeddedBrokerRule kafkaEmbeddedBrokerRule = new KafkaEmbeddedBrokerRule(1);

	@Override
	public KafkaEmbeddedBrokerRule getKafkaRule() {
		return kafkaEmbeddedBrokerRule;
	}

	@Test
	public void testLowVolumeLowConcurrency() throws Exception {
		createTopic(TEST_TOPIC, 5, 1, 1);

		ConnectionFactory connectionFactory = getKafkaBrokerConnectionFactory();
		ArrayList<Partition> readPartitions = new ArrayList<Partition>();
		for (int i = 0; i < 5; i++) {
			readPartitions.add(new Partition(TEST_TOPIC, i));
		}

		final KafkaMessageListenerContainer kafkaMessageListenerContainer = new KafkaMessageListenerContainer(connectionFactory, readPartitions.toArray(new Partition[readPartitions.size()]));
		kafkaMessageListenerContainer.setMaxFetchSizeInBytes(100);
		kafkaMessageListenerContainer.setConcurrency(2);

		int expectedMessageCount = 100;

		final MutableListMultimap<Integer,KeyedMessageWithOffset> receivedData = new SynchronizedPutFastListMultimap<Integer, KeyedMessageWithOffset>();
		final CountDownLatch latch = new CountDownLatch(expectedMessageCount);

		KafkaInboundChannelAdapter kafkaInboundChannelAdapter = new KafkaInboundChannelAdapter(kafkaMessageListenerContainer);

		StringDecoder decoder = new StringDecoder();
		kafkaInboundChannelAdapter.setKeyDecoder(decoder);
		kafkaInboundChannelAdapter.setPayloadDecoder(decoder);
		kafkaInboundChannelAdapter.setOutputChannel(new MessageChannel() {
			@Override
			public boolean send(Message<?> message) {
				latch.countDown();
				return receivedData.put(
						(Integer)message.getHeaders().get(KAFKA_MESSAGE_PARTITION),
						new KeyedMessageWithOffset(
								(String)message.getHeaders().get(KAFKA_MESSAGE_KEY),
								(String)message.getPayload(),
								(Long)message.getHeaders().get(KAFKA_MESSAGE_OFFSET),
								Thread.currentThread().getName(),
								(Integer)message.getHeaders().get(KAFKA_MESSAGE_PARTITION)));
			}


			@Override
			public boolean send(Message<?> message, long timeout) {
				return send(message);
			}
		});

		kafkaInboundChannelAdapter.afterPropertiesSet();
		kafkaInboundChannelAdapter.start();

		createStringProducer(NoCompressionCodec$.MODULE$.codec()).send(createMessages(100));

		latch.await((expectedMessageCount/5000) + 1, TimeUnit.MINUTES);
		kafkaMessageListenerContainer.stop();

		assertThat(receivedData.valuesView().toList(), hasSize(expectedMessageCount));
		assertThat(latch.getCount(), equalTo(0L));
		System.out.println("All messages received ... checking ");

		validateMessageReceipt(receivedData, 2, 5, 100, expectedMessageCount, readPartitions, 1);

	}

}

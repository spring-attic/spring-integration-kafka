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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.mail.search.IntegerComparisonTerm;

import com.gs.collections.api.RichIterable;
import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.map.MutableMap;
import com.gs.collections.api.multimap.list.MutableListMultimap;
import com.gs.collections.api.tuple.Pair;
import com.gs.collections.impl.block.factory.Functions;
import com.gs.collections.impl.factory.Maps;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.multimap.list.SynchronizedPutFastListMultimap;
import com.gs.collections.impl.utility.Iterate;
import kafka.message.NoCompressionCodec$;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.integration.kafka.core.KafkaBrokerConnectionFactory;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.inbound.KafkaInboundChannelAdapter;
import org.springframework.integration.kafka.serializer.common.StringDecoder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

/**
 * @author Marius Bogoevici
 */
public class TestKafkaInboundChannelAdapterWithSpecialOffset extends AbstractMessageListenerContainerTest {

	@Rule
	public final KafkaEmbeddedBrokerRule kafkaEmbeddedBrokerRule = new KafkaEmbeddedBrokerRule(1);

	@Override
	public KafkaEmbeddedBrokerRule getKafkaRule() {
		return kafkaEmbeddedBrokerRule;
	}

	@Test
	public void testLowVolumeLowConcurrency() throws Exception {

		// we will send 300 messages: first 200, then another 100
		// we will start reading from all partitions at offset 100
		int expectedMessageCount = 200;

		createTopic(TEST_TOPIC, 5, 1, 1);

		KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory = getKafkaBrokerConnectionFactory();
		ArrayList<Partition> readPartitions = new ArrayList<Partition>();
		Map<Partition,Long> startingOffsets = new HashMap<Partition, Long>();
		for (int i = 0; i < 5; i++) {
			Partition partition = new Partition(TEST_TOPIC, i);
			readPartitions.add(partition);
			startingOffsets.put(partition, 20L);
		}

		final KafkaMessageListenerContainer kafkaMessageListenerContainer = new KafkaMessageListenerContainer(kafkaBrokerConnectionFactory, readPartitions.toArray(new Partition[readPartitions.size()]));
		kafkaMessageListenerContainer.setMaxSize(100);
		kafkaMessageListenerContainer.setConcurrency(2);
		MetadataStoreOffsetManager offsetManager = new MetadataStoreOffsetManager(kafkaBrokerConnectionFactory, startingOffsets);
		kafkaMessageListenerContainer.setOffsetManager(offsetManager);

		// we send 100 messages
		createStringProducer(NoCompressionCodec$.MODULE$.codec()).send(createMessagesInRange(0, 199));

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

		createStringProducer(NoCompressionCodec$.MODULE$.codec()).send(createMessagesInRange(200, 299));

		latch.await((expectedMessageCount/5000) + 1, TimeUnit.MINUTES);
		kafkaMessageListenerContainer.stop();

		assertThat(receivedData.valuesView().toList(), hasSize(expectedMessageCount));
		assertThat(latch.getCount(), equalTo(0L));
		System.out.println("All messages received ... checking ");

		validateMessageReceipt(receivedData, 2, 5, 100, expectedMessageCount, readPartitions, 1);

		// For all received messages
		Collection<KeyedMessageWithOffset> allReceivedMessages = Iterate.flatCollect(receivedData.keyMultiValuePairsView(), new Function<Pair<Integer, RichIterable<KeyedMessageWithOffset>>, RichIterable<KeyedMessageWithOffset>>() {
			@Override
			public RichIterable<KeyedMessageWithOffset> valueOf(Pair<Integer, RichIterable<KeyedMessageWithOffset>> object) {
				return object.getTwo();
			}
		});

		// We extract the sequence value, i.e. "Message xx"
		Integer minValueInMessage = FastList.newList(allReceivedMessages).collect(new Function<KeyedMessageWithOffset, Integer>() {
			@Override
			public Integer valueOf(KeyedMessageWithOffset object) {
				return Integer.parseInt(object.getPayload().split(" ")[1]);
			}
		}).min();

		// The lowest received value is 100. That is correct, because messages are evenly distributed across partitions
		// and we start reading only at partition 200
		assertThat(minValueInMessage, equalTo(100));
	}

}

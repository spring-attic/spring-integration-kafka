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

import static scala.collection.JavaConversions.asScalaBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.gs.collections.api.RichIterable;
import com.gs.collections.api.block.function.Function2;
import com.gs.collections.api.multimap.Multimap;
import com.gs.collections.api.multimap.MutableMultimap;
import com.gs.collections.api.tuple.Pair;
import com.gs.collections.impl.factory.Multimaps;
import com.gs.collections.impl.tuple.Tuples;
import kafka.admin.AdminUtils;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import kafka.utils.TestUtils;
import org.junit.After;
import scala.collection.JavaConversions;
import scala.collection.Map;
import scala.collection.immutable.List$;
import scala.collection.immutable.Map$;
import scala.collection.immutable.Seq;

import org.springframework.integration.kafka.core.KafkaBrokerConnectionFactory;
import org.springframework.integration.kafka.core.KafkaConfiguration;

/**
 * @author Marius Bogoevici
 */
public abstract class AbstractBrokerTest {

	public static final String TEST_TOPIC = "test-topic";

	public abstract KafkaEmbeddedBrokerRule getKafkaRule();

	@After
	public void cleanUpTopic() {
		AdminUtils.deleteTopic(getKafkaRule().getZookeeperClient(), TEST_TOPIC);
		TestUtils.waitUntilMetadataIsPropagated(asScalaBuffer(getKafkaRule().getKafkaServers()), TEST_TOPIC, 0, 5000L);
	}

	public void createTopic(String topicName, int partitionCount, int brokers, int replication) {
		MutableMultimap<Integer, Integer> partitionDistribution = createPartitionDistribution(partitionCount, brokers, replication);
		AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(getKafkaRule().getZookeeperClient(), topicName, toKafkaPartitionMap(partitionDistribution), AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK$default$4(), AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK$default$5());
		for (int i = 0; i < partitionDistribution.keysView().size(); i++) {
			TestUtils.waitUntilMetadataIsPropagated(asScalaBuffer(getKafkaRule().getKafkaServers()), TEST_TOPIC, i, 5000L);
		}
	}

	public MutableMultimap<Integer, Integer> createPartitionDistribution(int partitionCount, int brokers, int replication) {
		MutableMultimap<Integer, Integer> partitionDistribution = Multimaps.mutable.list.with();
		for (int i = 0; i < partitionCount; i++) {
			for (int j = 0; j < replication; j++) {
				partitionDistribution.put(i, (i + j) % brokers);
			}
		}
		return partitionDistribution;
	}


	public KafkaConfiguration getKafkaConfiguration() {
		return new KafkaConfiguration(getKafkaRule().getBrokerAddresses());
	}

	public static scala.collection.Seq<KeyedMessage<String, String>> createMessages(int count) {
		return createMessagesInRange(0,count-1);
	}

	public static scala.collection.Seq<KeyedMessage<String, String>> createMessagesInRange(int start, int end) {
		List<KeyedMessage<String,String>> messages = new ArrayList<KeyedMessage<String, String>>();
		for (int i=start; i<= end; i++) {
			messages.add(new KeyedMessage<String, String>(TEST_TOPIC, "Key " + i, i, "Message " + i));
		}
		return asScalaBuffer(messages).toSeq();
	}

	public Producer<String, String> createStringProducer(int compression) {
		Properties producerConfig = TestUtils.getProducerConfig(getKafkaRule().getBrokersAsString(), TestPartitioner.class.getCanonicalName());
		producerConfig.put("serializer.class", StringEncoder.class.getCanonicalName());
		producerConfig.put("key.serializer.class",  StringEncoder.class.getCanonicalName());
		producerConfig.put("compression.codec",  Integer.toString(compression));
		return new Producer<String, String>(new ProducerConfig(producerConfig));
	}

	public KafkaBrokerConnectionFactory getKafkaBrokerConnectionFactory() throws Exception {
		KafkaBrokerConnectionFactory kafkaBrokerConnectionFactory = new KafkaBrokerConnectionFactory(getKafkaConfiguration());
		kafkaBrokerConnectionFactory.afterPropertiesSet();
		return kafkaBrokerConnectionFactory;
	}

	private Map toKafkaPartitionMap(Multimap<Integer, Integer> partitions) {
		java.util.Map<Object, Seq<Object>> m = partitions.toMap().collect(new Function2<Integer, RichIterable<Integer>, Pair<Object, Seq<Object>>>() {
			@Override
			public Pair<Object, Seq<Object>> value(Integer argument1, RichIterable<Integer> argument2) {
				return Tuples.pair((Object) argument1, List$.MODULE$.fromArray(argument2.toArray(new Object[0])).toSeq());
			}
		});
		return Map$.MODULE$.apply(JavaConversions.asScalaMap(m).toSeq());
	}


}

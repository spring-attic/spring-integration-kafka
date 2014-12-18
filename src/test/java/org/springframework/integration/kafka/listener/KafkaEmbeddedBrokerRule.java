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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.utility.ListIterate;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.Utils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.junit.rules.ExternalResource;
import scala.collection.JavaConversions;

import org.springframework.integration.kafka.core.KafkaBrokerAddress;

/**
 * @author Marius Bogoevici
 */
public class KafkaEmbeddedBrokerRule extends ExternalResource {

	private int count;

	private boolean controlledShutdown;

	private List<Integer> kafkaPorts;

	private List<KafkaServer> kafkaServers;

	private EmbeddedZookeeper zookeeper;

	private ZkClient zookeeperClient;

	@SuppressWarnings("unchecked")
	public KafkaEmbeddedBrokerRule(int count, boolean controlledShutdown) {
		this.count = count;
		this.controlledShutdown = controlledShutdown;
		this.kafkaPorts = JavaConversions.asJavaList((scala.collection.immutable.List) TestUtils.choosePorts(count));
	}

	public KafkaEmbeddedBrokerRule(int count) {
		this(count, false);
	}

	@Override
	protected void before() throws Throwable {
		zookeeper = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
		int zkConnectionTimeout = 6000;
		int zkSessionTimeout = 6000;
		zookeeperClient = new ZkClient(TestZKUtils.zookeeperConnect(), zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer$.MODULE$);
		kafkaServers = new ArrayList<KafkaServer>();
		for (int i = 0; i < count; i++) {
			Properties brokerConfigProperties = TestUtils.createBrokerConfig(i, kafkaPorts.get(i));
			brokerConfigProperties.put("controlled.shutdown.enable", Boolean.toString(controlledShutdown));
			KafkaServer server = TestUtils.createServer(new KafkaConfig(brokerConfigProperties), SystemTime$.MODULE$);
			kafkaServers.add(server);
		}
	}

	@Override
	protected void after() {
		for (KafkaServer kafkaServer : kafkaServers) {
			try {
				kafkaServer.shutdown();
			}
			catch (Exception e) {
				// do nothing
			}
			try {
				Utils.rm(kafkaServer.config().logDirs());
			}
			catch (Exception e) {
				// do nothing
			}
		}
		try {
			zookeeperClient.close();
		}
		catch (ZkInterruptedException e) {
			// do nothing
		}
		try {
			zookeeper.shutdown();
		}
		catch (Exception e) {
			// do nothing
		}
	}

	public List<KafkaServer> getKafkaServers() {
		return kafkaServers;
	}

	public KafkaServer getKafkaServer(int id) {
		return kafkaServers.get(id);
	}

	public EmbeddedZookeeper getZookeeper() {
		return zookeeper;
	}

	public ZkClient getZookeeperClient() {
		return zookeeperClient;
	}

	public List<KafkaBrokerAddress> getBrokerAddresses() {
		return ListIterate.collect(kafkaServers, new Function<KafkaServer, KafkaBrokerAddress>() {
			@Override
			public KafkaBrokerAddress valueOf(KafkaServer kafkaServer) {
				return new KafkaBrokerAddress(kafkaServer.config().hostName(), kafkaServer.config().port());
			}
		});
	}

	public void bounce(KafkaBrokerAddress kafkaBrokerAddress) {
		for (KafkaServer kafkaServer : getKafkaServers()) {
			if (kafkaBrokerAddress.equals(new KafkaBrokerAddress(kafkaServer.config().hostName(), kafkaServer.config().port()))) {
				kafkaServer.shutdown();
			}
		}
	}

	public String getBrokersAsString() {
		return FastList.newList(getBrokerAddresses()).collect(new Function<KafkaBrokerAddress, String>() {
			@Override
			public String valueOf(KafkaBrokerAddress object) {
				return object.getHost() + ":" + object.getPort();
			}
		}).makeString(",");
	}
}

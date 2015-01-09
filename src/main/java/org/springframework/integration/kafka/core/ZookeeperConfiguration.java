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

import com.gs.collections.api.block.function.Function;
import com.gs.collections.impl.list.mutable.FastList;
import kafka.cluster.Broker;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils$;
import org.I0Itec.zkclient.ZkClient;
import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * Kafka {@link Configuration} that uses a ZooKeeper connection for retrieving the list of seed brokers.
 *
 * @author Marius Bogoevici
 */
public class ZookeeperConfiguration extends AbstractConfiguration {

	public static final BrokerToBrokerAddressFunction brokerToBrokerAddressFunction = new BrokerToBrokerAddressFunction();

	private int connectionTimeout;

	private String zookeperServers;

	public ZookeeperConfiguration(String zookeperServers, int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
		this.zookeperServers = zookeperServers;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public String getZookeperServers() {
		return zookeperServers;
	}

	public void setZookeperServers(String zookeperServers) {
		this.zookeperServers = zookeperServers;
	}

	@Override
	protected List<BrokerAddress> doGetBrokerAddresses() {
		Seq<Broker> allBrokersInCluster = ZkUtils$.MODULE$.getAllBrokersInCluster(new ZkClient(zookeperServers, connectionTimeout, connectionTimeout, ZKStringSerializer$.MODULE$));
		FastList<Broker> brokers = FastList.newList(JavaConversions.asJavaCollection(allBrokersInCluster));
		return brokers.collect(brokerToBrokerAddressFunction);
	}

	@SuppressWarnings("serial")
	private static class BrokerToBrokerAddressFunction implements Function<Broker, BrokerAddress> {
		@Override
		public BrokerAddress valueOf(Broker broker) {
			return new BrokerAddress(broker.host(), broker.port());
		}
	}
}

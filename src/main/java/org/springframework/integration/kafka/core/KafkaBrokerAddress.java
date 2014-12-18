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

import org.springframework.util.StringUtils;

/**
 * Encapsulates the address of a Kafka broker
 *
 * @author Marius Bogoevici
 */
public class KafkaBrokerAddress {

	public static final int DEFAULT_PORT = 9092;

	private String host;

	private int port;

	public KafkaBrokerAddress(String host, int port) {
		if (StringUtils.isEmpty(host)) {
			throw new IllegalArgumentException("Host cannot be empty");
		}
		this.host = host;
		this.port = port;
	}

	public KafkaBrokerAddress(String host) {
		this(host, DEFAULT_PORT);
	}

	public static KafkaBrokerAddress fromAddress(String address)  {
		String[] split = address.split(":");
		if (split.length == 0 || split.length > 2) {
			throw new IllegalArgumentException("Expected format <host>[:<port>]");
		}
		if (split.length == 2) {
			return new KafkaBrokerAddress(split[0], Integer.parseInt(split[1]));
		} else {
			return new KafkaBrokerAddress(split[0]);
		}

	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		KafkaBrokerAddress kafkaBrokerAddress = (KafkaBrokerAddress) o;

		if (port != kafkaBrokerAddress.port) {
			return false;
		}
		if (!host.equals(kafkaBrokerAddress.host)) {
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		return 31 * host.hashCode() + port;
	}

	@Override
	public String toString() {
		return "KafkaBrokerAddress{" +
				"host='" + host + '\'' +
				", port=" + port +
				'}';
	}
}



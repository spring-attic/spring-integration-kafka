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

package org.springframework.integration.kafka.core;

import org.springframework.util.Assert;

/**
 * A reference to a Kafka partition, with both topic and partition id
 *
 * @author Marius Bogoevici
 */
public class Partition {

	private String topic;

	private int id;

	public Partition(String topic, int id) {
		Assert.hasText(topic, "Topic name cannot be empty");
		Assert.isTrue(id >= 0, "Partition id must be greater than or equal to 0");
		this.topic = topic;
		this.id = id;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	@Override
	public int hashCode() {
		int result = topic.hashCode();
		result = 31 * result + id;
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Partition partition = (Partition) o;
		if (id != partition.id) {
			return false;
		}
		return topic.equals(partition.topic);
	}

	@Override
	public String toString() {
		return "Partition[" + "topic='" + topic + '\'' + ", id=" + id + ']';
	}

}

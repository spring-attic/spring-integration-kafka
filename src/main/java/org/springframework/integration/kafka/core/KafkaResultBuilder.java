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

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for building a {@link KafkaResult}
 *
 * @author Marius Bogoevici
 */
class KafkaResultBuilder<T> {

	private Map<Partition, T> result;

	private Map<Partition, Short> errors;

	public KafkaResultBuilder() {
		this.result = new HashMap<Partition, T>();
		this.errors = new HashMap<Partition, Short>();
	}

	public KafkaPartitionResultHolder add(Partition Partition) {
		return new KafkaPartitionResultHolder(Partition);
	}

	public KafkaResult<T> build() {
		return new KafkaResult<T>(result, errors);
	}

	class KafkaPartitionResultHolder {

		private Partition Partition;

		public KafkaPartitionResultHolder(Partition Partition) {
			this.Partition = Partition;
		}

		public KafkaResultBuilder withResult(T result) {
			if (KafkaResultBuilder.this.errors.containsKey(Partition)) {
				throw new IllegalArgumentException("A KafkaResult cannot contain both an error and a result for the same topic and partition");
			}
			KafkaResultBuilder.this.result.put(Partition, result);
			return KafkaResultBuilder.this;
		}

		public KafkaResultBuilder withError(short error) {
			if (KafkaResultBuilder.this.result.containsKey(Partition)) {
				throw new IllegalArgumentException("A FetchResult cannot contain both an error and a MessageSet for the same topic and partition");
			}
			KafkaResultBuilder.this.errors.put(Partition, error);
			return KafkaResultBuilder.this;
		}

	}
}

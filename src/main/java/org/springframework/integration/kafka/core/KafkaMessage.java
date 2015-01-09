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

import kafka.message.Message;

/**
 * Wrapper around a {@link KafkaMessage}, grouping the original {@link Message} content and metadata.
 *
 * @author Marius Bogoevici
 */
public class KafkaMessage {

	private final Message message;

	private final KafkaMessageMetadata metadata;

	public KafkaMessage(Message message, KafkaMessageMetadata metadata) {
		this.message = message;
		this.metadata = metadata;
	}

	public Message getMessage() {
		return message;
	}

	public KafkaMessageMetadata getMetadata() {
		return metadata;
	}

}

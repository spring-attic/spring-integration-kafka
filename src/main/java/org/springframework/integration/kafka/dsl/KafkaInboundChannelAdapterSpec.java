/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.kafka.dsl;

import org.springframework.integration.dsl.MessageSourceSpec;
import org.springframework.integration.kafka.inbound.KafkaMessageSource;
import org.springframework.integration.kafka.inbound.KafkaMessageSource.KafkaAckCallbackFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConsumerProperties;
import org.springframework.kafka.support.converter.RecordMessageConverter;

/**
 * Spec for a polled Kafka inbound channel adapter.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Anshul Mehra
 *
 * @since 3.0.1
 *
 */
public class KafkaInboundChannelAdapterSpec<K, V>
		extends MessageSourceSpec<KafkaInboundChannelAdapterSpec<K, V>, KafkaMessageSource<K, V>> {

	KafkaInboundChannelAdapterSpec(ConsumerFactory<K, V> consumerFactory,
			ConsumerProperties consumerProperties, boolean allowMultiFetch) {
		this.target = new KafkaMessageSource<>(consumerFactory, consumerProperties, allowMultiFetch);
	}

	KafkaInboundChannelAdapterSpec(ConsumerFactory<K, V> consumerFactory,
			ConsumerProperties consumerProperties,
			KafkaAckCallbackFactory<K, V> ackCallbackFactory, boolean allowMultiFetch) {

		this.target = new KafkaMessageSource<>(consumerFactory, consumerProperties, ackCallbackFactory, allowMultiFetch);
	}

	public KafkaInboundChannelAdapterSpec<K, V> messageConverter(RecordMessageConverter messageConverter) {
		this.target.setMessageConverter(messageConverter);
		return this;
	}

	public KafkaInboundChannelAdapterSpec<K, V> payloadType(Class<?> type) {
		this.target.setPayloadType(type);
		return this;
	}

	public KafkaInboundChannelAdapterSpec<K, V> rawMessageHeader(boolean rawMessageHeader) {
		this.target.setRawMessageHeader(rawMessageHeader);
		return this;
	}

}

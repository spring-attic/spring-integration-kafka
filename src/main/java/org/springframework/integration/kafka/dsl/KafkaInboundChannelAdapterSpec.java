/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.kafka.dsl;

import java.lang.reflect.Type;
import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.integration.dsl.MessageSourceSpec;
import org.springframework.integration.kafka.inbound.KafkaMessageSource;
import org.springframework.integration.kafka.inbound.KafkaMessageSource.KafkaAckCallbackFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.converter.RecordMessageConverter;

/**
 * Spec for a polled Kafka inbound channel adapter.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @param <S> the target {@link KafkaInboundChannelAdapterSpec} implementation type.
 *
 * @author Gary Russell
 *
 * @since 3.0.1
 *
 */
public class KafkaInboundChannelAdapterSpec<K, V, S extends KafkaInboundChannelAdapterSpec<K, V, S>>
		extends MessageSourceSpec<KafkaInboundChannelAdapterSpec<K, V, S>, KafkaMessageSource<K, V>> {

	KafkaInboundChannelAdapterSpec(ConsumerFactory<K, V> consumerFactory, String... topics) {
		this.target = new KafkaMessageSource<K, V>(consumerFactory, topics);
	}

	KafkaInboundChannelAdapterSpec(ConsumerFactory<K, V> consumerFactory,
			KafkaAckCallbackFactory<K, V> ackCallbackFactory, String... topics) {

		this.target = new KafkaMessageSource<K, V>(consumerFactory, ackCallbackFactory, topics);
	}

	public KafkaInboundChannelAdapterSpec<K, V, S> setHeaderExpressions(Map<String, Expression> headerExpressions) {
		this.target.setHeaderExpressions(headerExpressions);
		return this;
	}

	public KafkaInboundChannelAdapterSpec<K, V, S> setGroupId(String groupId) {
		this.target.setGroupId(groupId);
		return this;
	}

	public KafkaInboundChannelAdapterSpec<K, V, S> setClientId(String clientId) {
		this.target.setClientId(clientId);
		return this;
	}

	public KafkaInboundChannelAdapterSpec<K, V, S> setPollTimeout(long pollTimeout) {
		this.target.setPollTimeout(pollTimeout);
		return this;
	}

	public KafkaInboundChannelAdapterSpec<K, V, S> setMessageConverter(RecordMessageConverter messageConverter) {
		this.target.setMessageConverter(messageConverter);
		return this;
	}

	public KafkaInboundChannelAdapterSpec<K, V, S> setPayloadType(Type type) {
		this.target.setPayloadType(type);
		return this;
	}

}

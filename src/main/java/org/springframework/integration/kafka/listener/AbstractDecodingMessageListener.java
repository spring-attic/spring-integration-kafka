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

import static org.springframework.integration.kafka.util.MessageUtils.decodeKey;
import static org.springframework.integration.kafka.util.MessageUtils.decodePayload;

import kafka.serializer.Decoder;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.integration.kafka.core.KafkaMessage;
import org.springframework.integration.kafka.core.KafkaMessageMetadata;
import org.springframework.util.Assert;

/**
 * @author Marius Bogoevici
 */
public abstract class AbstractDecodingMessageListener<K, P> implements MessageListener, InitializingBean {

	private Decoder<K> keyDecoder;

	private Decoder<P> payloadDecoder;

	public AbstractDecodingMessageListener(Decoder<K> keyDecoder, Decoder<P> payloadDecoder) {
		this.keyDecoder = keyDecoder;
		this.payloadDecoder = payloadDecoder;
	}

	public Decoder<K> getKeyDecoder() {
		return keyDecoder;
	}

	public void setKeyDecoder(Decoder<K> keyDecoder) {
		this.keyDecoder = keyDecoder;
	}

	public Decoder<P> getPayloadDecoder() {
		return payloadDecoder;
	}

	public void setPayloadDecoder(Decoder<P> payloadDecoder) {
		this.payloadDecoder = payloadDecoder;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(keyDecoder, "Key decoder decoder cannot be null");
		Assert.notNull(payloadDecoder, "Payload decoder cannot be null");
	}

	@Override
	public final void onMessage(KafkaMessage message) {
		this.doOnMessage(decodeKey(message, keyDecoder), decodePayload(message, payloadDecoder), message.getMetadata());
	}

	public abstract void doOnMessage(K key, P payload, KafkaMessageMetadata metadata);

}

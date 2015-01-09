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


package org.springframework.integration.kafka.inbound;

import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;

import org.springframework.integration.context.OrderlyShutdownCapable;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.kafka.core.KafkaMessageMetadata;
import org.springframework.integration.kafka.listener.AbstractDecodingMessageListener;
import org.springframework.integration.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

/**
 * @author Marius Bogoevici
 */
public class KafkaInboundChannelAdapter extends MessageProducerSupport implements OrderlyShutdownCapable {

	public static final String KAFKA_MESSAGE_KEY = "kafka.message.key";

	public static final String KAFKA_MESSAGE_TOPIC = "kafka.message.topic";

	public static final String KAFKA_MESSAGE_PARTITION = "kafka.message.partition.id";

	public static final String KAFKA_MESSAGE_OFFSET = "kafka.message.offset";

	private KafkaMessageListenerContainer messageListenerContainer;

	private Decoder<?> keyDecoder = new DefaultDecoder(null);

	private Decoder<?> payloadDecoder = new DefaultDecoder(null);

	public KafkaInboundChannelAdapter(KafkaMessageListenerContainer messageListenerContainer) {
		Assert.notNull(messageListenerContainer);
		Assert.isNull(messageListenerContainer.getMessageListener());
		this.messageListenerContainer = messageListenerContainer;
		this.messageListenerContainer.setAutoStartup(false);
	}

	public Decoder<?> getKeyDecoder() {
		return keyDecoder;
	}

	public void setKeyDecoder(Decoder<?> keyDecoder) {
		this.keyDecoder = keyDecoder;
	}

	public Decoder<?> getPayloadDecoder() {
		return payloadDecoder;
	}

	public void setPayloadDecoder(Decoder<?> payloadDecoder) {
		this.payloadDecoder = payloadDecoder;
	}

	@Override
	protected void onInit() {
		this.messageListenerContainer.setMessageListener(new ChannelForwardingMessageListener());
		super.onInit();
	}

	@Override
	protected void doStart() {
		this.messageListenerContainer.start();
	}

	@Override
	protected void doStop() {
		this.messageListenerContainer.stop();
	}

	@Override
	public String getComponentType() {
		return "amqp:inbound-channel-adapter";
	}

	@Override
	public int beforeShutdown() {
		this.messageListenerContainer.stop();
		return 0;
	}

	@Override
	public int afterShutdown() {
		return 0;
	}

	private class ChannelForwardingMessageListener extends AbstractDecodingMessageListener {

		public ChannelForwardingMessageListener() {
			super(keyDecoder, payloadDecoder);
		}

		@Override
		public void doOnMessage(Object key, Object payload, KafkaMessageMetadata metadata) {
			Message<Object> message = MessageBuilder
					.withPayload(payload)
					.setHeader(KAFKA_MESSAGE_KEY, key)
					.setHeader(KAFKA_MESSAGE_TOPIC, metadata.getPartition().getTopic())
					.setHeader(KAFKA_MESSAGE_PARTITION, metadata.getPartition().getId())
					.setHeader(KAFKA_MESSAGE_OFFSET, metadata.getOffset())
					.build();
			KafkaInboundChannelAdapter.this.sendMessage(message);
		}
	}
}

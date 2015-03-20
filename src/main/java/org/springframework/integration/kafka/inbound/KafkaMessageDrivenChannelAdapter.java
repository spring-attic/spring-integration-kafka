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

package org.springframework.integration.kafka.inbound;

import java.util.Map;
import java.util.UUID;

import com.gs.collections.api.map.MutableMap;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;

import org.springframework.integration.context.OrderlyShutdownCapable;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.kafka.core.KafkaMessageMetadata;
import org.springframework.integration.kafka.listener.AbstractDecodingAcknowledgingMessageListener;
import org.springframework.integration.kafka.listener.AbstractDecodingMessageListener;
import org.springframework.integration.kafka.listener.Acknowledgment;
import org.springframework.integration.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.integration.kafka.support.KafkaHeaders;
import org.springframework.integration.support.DefaultMessageBuilderFactory;
import org.springframework.integration.support.MessageBuilderFactory;
import org.springframework.integration.support.MutableMessageBuilderFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;

/**
 * @author Marius Bogoevici
 */
public class KafkaMessageDrivenChannelAdapter extends MessageProducerSupport implements OrderlyShutdownCapable {

	private final KafkaMessageListenerContainer messageListenerContainer;

	private Decoder<?> keyDecoder = new DefaultDecoder(null);

	private Decoder<?> payloadDecoder = new DefaultDecoder(null);

	private boolean generateMessageId = false;

	private boolean generateTimestamp = false;

	private boolean useMessageBuilderFactory = false;

	private boolean autoCommitOffset = true;

	public KafkaMessageDrivenChannelAdapter(KafkaMessageListenerContainer messageListenerContainer) {
		Assert.notNull(messageListenerContainer);
		Assert.isNull(messageListenerContainer.getMessageListener());
		this.messageListenerContainer = messageListenerContainer;
		this.messageListenerContainer.setAutoStartup(false);
	}

	public void setKeyDecoder(Decoder<?> keyDecoder) {
		this.keyDecoder = keyDecoder;
	}

	public void setPayloadDecoder(Decoder<?> payloadDecoder) {
		this.payloadDecoder = payloadDecoder;
	}

	public void setAutoCommitOffset(boolean autoCommitOffset) {
		this.autoCommitOffset = autoCommitOffset;
	}

	/**
	 * Generate Message Ids for produced messages. If set to false, will try to use a default value. By default set to false.
	 * Please note that this option only works in conjunction with {@link #setUseMessageBuilderFactory(boolean)}. If the
	 * latter is set to true, then some {@link MessageBuilderFactory} implementations such as
	 * {@link DefaultMessageBuilderFactory} may ignore it.
	 *
	 * @param generateMessageId true if a message id should be generated
	 */
	public void setGenerateMessageId(boolean generateMessageId) {
		this.generateMessageId = generateMessageId;
	}

	/**
	 * Generate timestamp for produced messages. If set to false, -1 is used instead. By default set to false.
	 * Please note that this option only works in conjunction with {@link #setUseMessageBuilderFactory(boolean)}. If the
	 * latter is set to true, then some {@link MessageBuilderFactory} implementations such as
	 * {@link DefaultMessageBuilderFactory} may ignore it.
	 *
	 * @param generateTimestamp true if a timestamp should be generated
	 */
	public void setGenerateTimestamp(boolean generateTimestamp) {
		this.generateTimestamp = generateTimestamp;
	}

	/**
	 * Use the {@link MessageBuilderFactory} returned by {@link #getMessageBuilderFactory()} to create messages.
	 *
	 * @param useMessageBuilderFactory true if the @link MessageBuilderFactory} returned by
	 * {@link #getMessageBuilderFactory()} should be used.
	 */
	public void setUseMessageBuilderFactory(boolean useMessageBuilderFactory) {
		this.useMessageBuilderFactory = useMessageBuilderFactory;
	}

	@Override
	protected void onInit() {
		this.messageListenerContainer.setMessageListener(autoCommitOffset ?
				new AutoAcknowledgingChannelForwardingMessageListener()
				: new AcknowledgingChannelForwardingMessageListener());
		if (!generateMessageId && !generateTimestamp
				&& (getMessageBuilderFactory() instanceof DefaultMessageBuilderFactory)) {
			setMessageBuilderFactory(new MutableMessageBuilderFactory());
		}
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
		return "kafka:message-driven-channel-adapter";
	}

	@Override
	public int beforeShutdown() {
		this.messageListenerContainer.stop();
		return getPhase();
	}

	@Override
	public int afterShutdown() {
		return getPhase();
	}

	@SuppressWarnings("rawtypes")
	private class AutoAcknowledgingChannelForwardingMessageListener extends AbstractDecodingMessageListener {

		@SuppressWarnings("unchecked")
		public AutoAcknowledgingChannelForwardingMessageListener() {
			super(keyDecoder, payloadDecoder);
		}

		@Override
		public void doOnMessage(Object key, Object payload, KafkaMessageMetadata metadata) {
			KafkaMessageDrivenChannelAdapter.this.sendMessage(toMessage(key, payload, metadata, null));
		}

	}

	@SuppressWarnings("rawtypes")
	private class AcknowledgingChannelForwardingMessageListener extends AbstractDecodingAcknowledgingMessageListener {

		@SuppressWarnings("unchecked")
		public AcknowledgingChannelForwardingMessageListener() {
			super(keyDecoder, payloadDecoder);
		}

		@Override
		public void doOnMessage(Object key, Object payload, KafkaMessageMetadata metadata,
				Acknowledgment acknowledgment) {
			KafkaMessageDrivenChannelAdapter.this.sendMessage(toMessage(key, payload, metadata, acknowledgment));
		}

	}

	private Message<Object> toMessage(Object key, final Object payload, KafkaMessageMetadata metadata,
			Acknowledgment acknowledgment) {

		final MutableMap<String, Object> headers = UnifiedMap.<String, Object>newMap()
				.withKeyValue(KafkaHeaders.MESSAGE_KEY, key)
				.withKeyValue(KafkaHeaders.TOPIC, metadata.getPartition().getTopic())
				.withKeyValue(KafkaHeaders.PARTITION_ID, metadata.getPartition().getId())
				.withKeyValue(KafkaHeaders.OFFSET, metadata.getOffset())
				.withKeyValue(KafkaHeaders.NEXT_OFFSET, metadata.getNextOffset());

		// pre-set the message id header if set to not generate
		if (!generateMessageId) {
			headers.withKeyValue(MessageHeaders.ID, MessageHeaders.ID_VALUE_NONE);
		}

		// pre-set the timestamp header if set to not generate
		if (!generateTimestamp) {
			headers.withKeyValue(MessageHeaders.TIMESTAMP, -1L);
		}

		if (!autoCommitOffset) {
			headers.put(KafkaHeaders.ACKNOWLEDGMENT, acknowledgment);
		}

		if (useMessageBuilderFactory) {
			return getMessageBuilderFactory()
					.withPayload(payload)
					.copyHeaders(headers)
					.build();
		}
		else {
			return new KafkaMessage(payload, headers);
		}

	}

	/**
	 * Special subclass of {@link Message}. We use this for lower message generation overhead, unless the default
	 * strategy of the superclass is set via {@link #setUseMessageBuilderFactory(boolean)}
	 *
	 */
	private class KafkaMessage implements Message<Object> {

		private final Object payload;

		private final MessageHeaders messageHeaders;

		public KafkaMessage(Object payload, MutableMap<String, Object> headers) {
			this.payload = payload;
			this.messageHeaders = new KafkaMessageHeaders(headers, generateMessageId, generateTimestamp);
		}

		@Override
		public Object getPayload() {
			return payload;
		}

		@Override
		public MessageHeaders getHeaders() {
			return messageHeaders;
		}

	}

	private class KafkaMessageHeaders extends MessageHeaders {

		public KafkaMessageHeaders(Map<String, Object> headers, boolean generateId, boolean generateTimestamp) {
			super(headers, generateId ? null : ID_VALUE_NONE, generateTimestamp ? null : -1L);
		}

	}

}

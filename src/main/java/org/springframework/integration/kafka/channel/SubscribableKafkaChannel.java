/*
 * Copyright 2020 the original author or authors.
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

package org.springframework.integration.kafka.channel;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.context.Phased;
import org.springframework.context.SmartLifecycle;
import org.springframework.integration.dispatcher.BroadcastingDispatcher;
import org.springframework.integration.dispatcher.MessageDispatcher;
import org.springframework.integration.dispatcher.RoundRobinLoadBalancingStrategy;
import org.springframework.integration.dispatcher.UnicastingDispatcher;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;

/**
 * Subscribable channel backed by a Kafka topic.
 *
 * @author Gary Russell
 * @since 3.3
 *
 */
public class SubscribableKafkaChannel extends AbstractKafkaChannel implements SubscribableChannel, SmartLifecycle {

	private static final int DEFAULT_PHASE = Integer.MAX_VALUE / 2; // same as MessageProducerSupport

	private final KafkaListenerContainerFactory<?> factory;

	private final boolean pubSub;

	private MessageDispatcher dispatcher;

	private MessageListenerContainer container;

	private boolean autoStartup = true;

	private int phase = DEFAULT_PHASE;

	private volatile boolean running;

	/**
	 * Construct an instance with the provided parameters.
	 * @param template template for sending.
	 * @param factory factory for creating a container for receiving.
	 * @param channelTopic the topic.
	 */
	public SubscribableKafkaChannel(KafkaTemplate<?, ?> template, KafkaListenerContainerFactory<?> factory,
			String channelTopic) {

		this(template, factory, channelTopic, false);
	}

	/**
	 * Construct an instance with the provided parameters.
	 * @param template template for sending.
	 * @param factory factory for creating a container for receiving.
	 * @param channelTopic the topic.
	 * @param pubSub true for a publish/subscribe channel.
	 */
	public SubscribableKafkaChannel(KafkaTemplate<?, ?> template, KafkaListenerContainerFactory<?> factory,
			String channelTopic, boolean pubSub) {

		super(template, channelTopic);
		this.pubSub = pubSub;
		this.factory = factory;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	/**
	 * Set the phase.
	 * @param phase the phase.
	 * @see Phased
	 */
	public void setPhase(int phase) {
		this.phase = phase;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	/**
	 * Set the auto startup.
	 * @param autoStartup true to automatically start.
	 * @see SmartLifecycle
	 */
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	@Override
	protected void onInit() {
		if (this.pubSub) {
			BroadcastingDispatcher broadcastingDispatcher = new BroadcastingDispatcher(true);
			broadcastingDispatcher.setBeanFactory(this.getBeanFactory());
			this.dispatcher = broadcastingDispatcher;
		}
		else {
			UnicastingDispatcher unicastingDispatcher = new UnicastingDispatcher();
			unicastingDispatcher.setLoadBalancingStrategy(new RoundRobinLoadBalancingStrategy());
			this.dispatcher = unicastingDispatcher;
		}
		this.container = this.factory.createContainer(this.topic);
		String groupId = getGroupId();
		this.container.getContainerProperties().setGroupId(groupId != null ? groupId : getBeanName());
		this.container.getContainerProperties().setMessageListener(
				new RecordMessagingMessageListenerAdapter<Object, Object>(null, null) {

					@Override
					public void onMessage(ConsumerRecord<Object, Object> record, Acknowledgment acknowledgment,
							Consumer<?, ?> consumer) {

						SubscribableKafkaChannel.this.dispatcher
								.dispatch(toMessagingMessage(record, acknowledgment, consumer));
					}

		});
	}

	@Override
	public void start() {
		this.container.start();
		this.running = true;
	}

	@Override
	public void stop() {
		this.container.stop();
		this.running = false;
	}

	@Override
	public void stop(Runnable callback) {
		this.container.stop(() -> {
			callback.run();
			this.running = false;
		});
	}

	@Override
	public boolean subscribe(MessageHandler handler) {
		return this.dispatcher.addHandler(handler);
	}

	@Override
	public boolean unsubscribe(MessageHandler handler) {
		return this.dispatcher.removeHandler(handler);
	}

}

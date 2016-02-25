/*
 * Copyright 2014 the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.MessageListener;

/**
 * An abstract {@link MessageListener} adapter providing the necessary infrastructure
 * to extract the payload of a {@link Message}
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @since 1.4
 * @see MessageListener
 * @see ChannelAwareMessageListener
 */
public abstract class AbstractAdaptableMessageListener<K, V> implements MessageListener<K, V>,
			AcknowledgingMessageListener<K, V> {

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());


	/**
	 * Rabbit {@link MessageListener} entry point.
	 * <p>
	 * Delegates the message to the target listener method, with appropriate conversion of the message argument. In case
	 * of an exception, the {@link #handleListenerException(Throwable)} method will be invoked.
	 * <p>
	 * <b>Note:</b> Does not support sending response messages based on result objects returned from listener methods.
	 * Use the {@link ChannelAwareMessageListener} entry point (typically through a Spring message listener container)
	 * for handling result objects as well.
	 * @param message the incoming Rabbit message
	 * @see #handleListenerException
	 * @see #onMessage(Message, com.rabbitmq.client.Channel)
	 */
	@Override
	public void onMessage(ConsumerRecord<K, V> record) {
		try {
			onMessage(record, null);
		}
		catch (Exception ex) {
			handleListenerException(ex);
		}
	}

	/**
	 * Handle the given exception that arose during listener execution.
	 * The default implementation logs the exception at error level.
	 * <p>
	 * This method only applies when using a Rabbit {@link MessageListener}. With
	 * {@link ChannelAwareMessageListener}, exceptions get handled by the
	 * caller instead.
	 * @param ex the exception to handle
	 * @see #onMessage(Message)
	 */
	protected void handleListenerException(Throwable ex) {
		logger.error("Listener execution failed", ex);
	}

	/**
	 * Extract the message body from the given Kafka message.
	 * @param record the Rabbit <code>Message</code>
	 * @return the content of the message, to be passed into the listener method as argument
	 */
	protected Object extractMessage(ConsumerRecord<K, V> record) {
		return record.value();
	}

}

/*
 * Copyright 2016-2017 the original author or authors.
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

import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;

import org.springframework.integration.dsl.ComponentsRegistration;
import org.springframework.integration.dsl.MessageProducerSpec;
import org.springframework.integration.dsl.MessagingGatewaySpec;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenEndpoint;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.adapter.FilteringAcknowledgingMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.kafka.listener.adapter.RetryingAcknowledgingMessageListenerAdapter;
import org.springframework.kafka.support.converter.BatchMessageConverter;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

/**
 * A {@link MessageProducerSpec} implementation for the {@link KafkaMessageDrivenEndpoint}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @param <S> the target {@link KafkaMessageDrivenChannelAdapterSpec} implementation type.
 *
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 3.0
 */
public class KafkaMessageDrivenChannelAdapterSpec<K, V, S extends KafkaMessageDrivenChannelAdapterSpec<K, V, S>>
		extends MessagingGatewaySpec<S, KafkaMessageDrivenEndpoint<K, V>> {

	KafkaMessageDrivenChannelAdapterSpec(AbstractMessageListenerContainer<K, V> messageListenerContainer,
			KafkaMessageDrivenEndpoint.ListenerMode listenerMode) {
		super(new KafkaMessageDrivenEndpoint<>(messageListenerContainer, listenerMode));
	}

	/**
	 * Set the message converter; must be a {@link RecordMessageConverter} or
	 * {@link BatchMessageConverter} depending on mode.
	 * @param messageConverter the converter.
	 * @return the spec
	 */
	public S messageConverter(MessageConverter messageConverter) {
		this.target.setMessageConverter(messageConverter);
		return _this();
	}

	/**
	 * Set the message converter to use with a record-based consumer.
	 * @param messageConverter the converter.
	 * @return the spec
	 */
	public S recordMessageConverter(RecordMessageConverter messageConverter) {
		this.target.setRecordMessageConverter(messageConverter);
		return _this();
	}

	/**
	 * Set the message converter to use with a batch-based consumer.
	 * @param messageConverter the converter.
	 * @return the spec
	 */
	public S batchMessageConverter(BatchMessageConverter messageConverter) {
		this.target.setBatchMessageConverter(messageConverter);
		return _this();
	}

	/**
	 * Specify a {@link RecordFilterStrategy} to wrap
	 * {@code KafkaMessageDrivenChannelAdapter.IntegrationRecordMessageListener} into
	 * {@link FilteringAcknowledgingMessageListenerAdapter}.
	 * @param recordFilterStrategy the {@link RecordFilterStrategy} to use.
	 * @return the spec
	 */
	public S recordFilterStrategy(RecordFilterStrategy<K, V> recordFilterStrategy) {
		this.target.setRecordFilterStrategy(recordFilterStrategy);
		return _this();
	}

	/**
	 * A {@code boolean} flag to indicate if {@link FilteringAcknowledgingMessageListenerAdapter}
	 * should acknowledge discarded records or not.
	 * Does not make sense if {@link #recordFilterStrategy(RecordFilterStrategy)} isn't specified.
	 * @param ackDiscarded true to ack (commit offset for) discarded messages.
	 * @return the spec
	 */
	public S ackDiscarded(boolean ackDiscarded) {
		this.target.setAckDiscarded(ackDiscarded);
		return _this();
	}

	/**
	 * Specify a {@link RetryTemplate} instance to wrap
	 * {@code KafkaMessageDrivenChannelAdapter.IntegrationRecordMessageListener} into
	 * {@link RetryingAcknowledgingMessageListenerAdapter}.
	 * @param retryTemplate the {@link RetryTemplate} to use.
	 * @return the spec
	 */
	public S retryTemplate(RetryTemplate retryTemplate) {
		this.target.setRetryTemplate(retryTemplate);
		return _this();
	}

	/**
	 * A {@link RecoveryCallback} instance for retry operation;
	 * if null, the exception will be thrown to the container after retries are exhausted.
	 * Does not make sense if {@link #retryTemplate(RetryTemplate)} isn't specified.
	 * @param recoveryCallback the recovery callback.
	 * @return the spec
	 */
	public S recoveryCallback(RecoveryCallback<Void> recoveryCallback) {
		this.target.setRecoveryCallback(recoveryCallback);
		return _this();
	}

	/**
	 /**
	 * The {@code boolean} flag to specify the order how
	 * {@link RetryingAcknowledgingMessageListenerAdapter} and
	 * {@link FilteringAcknowledgingMessageListenerAdapter} are wrapped to each other,
	 * if both of them are present.
	 * Does not make sense if only one of {@link RetryTemplate} or
	 * {@link RecordFilterStrategy} is present, or any.
	 * @param filterInRetry the order for {@link RetryingAcknowledgingMessageListenerAdapter} and
	 * {@link FilteringAcknowledgingMessageListenerAdapter} wrapping. Defaults to {@code false}.
	 * @return the spec
	 */
	public S filterInRetry(boolean filterInRetry) {
		this.target.setFilterInRetry(filterInRetry);
		return _this();
	}

	/**
	 * A {@link ConcurrentMessageListenerContainer} configuration {@link KafkaMessageDrivenChannelAdapterSpec}
	 * extension.
	 * @param <K> the key type.
	 * @param <V> the value type.
	 */
	public static class KafkaMessageDrivenChannelAdapterListenerContainerSpec<K, V> extends
			KafkaMessageDrivenChannelAdapterSpec<K, V, KafkaMessageDrivenChannelAdapterListenerContainerSpec<K, V>>
			implements ComponentsRegistration {

		private final KafkaMessageListenerContainerSpec<K, V> spec;

		KafkaMessageDrivenChannelAdapterListenerContainerSpec(KafkaMessageListenerContainerSpec<K, V> spec,
				KafkaMessageDrivenEndpoint.ListenerMode listenerMode) {
			super(spec.getContainer(), listenerMode);
			this.spec = spec;
		}

		/**
		 * Configure a listener container by invoking the {@link Consumer} callback, with a
		 * {@link KafkaMessageListenerContainerSpec} argument.
		 * @param configurer the configurer Java 8 Lambda.
		 * @return the spec.
		 */
		public KafkaMessageDrivenChannelAdapterListenerContainerSpec<K, V> configureListenerContainer(
				Consumer<KafkaMessageListenerContainerSpec<K, V>> configurer) {
			Assert.notNull(configurer, "The 'configurer' cannot be null");
			configurer.accept(this.spec);
			return _this();
		}

		@Override
		public Collection<Object> getComponentsToRegister() {
			return Collections.singleton(this.spec.getContainer());
		}

	}

}

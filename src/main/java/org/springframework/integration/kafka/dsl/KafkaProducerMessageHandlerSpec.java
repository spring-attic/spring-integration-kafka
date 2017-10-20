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

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.dsl.ComponentsRegistration;
import org.springframework.integration.dsl.IntegrationComponentSpec;
import org.springframework.integration.dsl.MessageHandlerSpec;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaderMapper;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * A {@link MessageHandlerSpec} implementation for the {@link KafkaProducerMessageHandler}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @param <S> the {@link KafkaProducerMessageHandlerSpec} extension type.
 *
 * @author Artem Bilan
 * @author Biju Kunjummen
 *
 * @since 3.0
 */
public class KafkaProducerMessageHandlerSpec<K, V, S extends KafkaProducerMessageHandlerSpec<K, V, S>>
		extends MessageHandlerSpec<S, KafkaProducerMessageHandler<K, V>> {

	KafkaProducerMessageHandlerSpec(KafkaTemplate<K, V> kafkaTemplate) {
		this.target = new KafkaProducerMessageHandler<>(kafkaTemplate);
	}

	/**
	 * Configure the Kafka topic to send messages.
	 * @param topic the Kafka topic name.
	 * @return the spec.
	 */
	public S topic(String topic) {
		return topicExpression(new LiteralExpression(topic));
	}

	/**
	 * Configure a SpEL expression to determine the Kafka topic at runtime against
	 * request Message as a root object of evaluation context.
	 * @param topicExpression the topic SpEL expression.
	 * @return the spec.
	 */
	public S topicExpression(String topicExpression) {
		return topicExpression(PARSER.parseExpression(topicExpression));
	}

	/**
	 * Configure an {@link Expression} to determine the Kafka topic at runtime against
	 * request Message as a root object of evaluation context.
	 * @param topicExpression the topic expression.
	 * @return the spec.
	 */
	public S topicExpression(Expression topicExpression) {
		this.target.setTopicExpression(topicExpression);
		return _this();
	}

	/**
	 * Configure a {@link Function} that will be invoked at run time to determine the topic to
	 * which a message will be sent. Typically used with a Java 8 Lambda expression:
	 * <pre class="code">
	 * {@code
	 * .<Foo>topic(m -> m.getPayload().getTopic())
	 * }
	 * </pre>
	 * @param topicFunction the topic function.
	 * @param <P> the expected payload type.
	 * @return the current {@link KafkaProducerMessageHandlerSpec}.
	 * @see FunctionExpression
	 */
	public <P> S topic(Function<Message<P>, String> topicFunction) {
		return topicExpression(new FunctionExpression<>(topicFunction));
	}

	/**
	 * Configure a SpEL expression to determine the Kafka message key to store at runtime against
	 * request Message as a root object of evaluation context.
	 * @param messageKeyExpression the message key SpEL expression.
	 * @return the spec.
	 */
	public S messageKeyExpression(String messageKeyExpression) {
		return messageKeyExpression(PARSER.parseExpression(messageKeyExpression));
	}

	/**
	 * Configure the message key to store message in Kafka topic.
	 * @param messageKey the message key to use.
	 * @return the spec.
	 */
	public S messageKey(String messageKey) {
		return messageKeyExpression(new LiteralExpression(messageKey));
	}

	/**
	 * Configure an {@link Expression} to determine the Kafka message key to store at runtime against
	 * request Message as a root object of evaluation context.
	 * @param messageKeyExpression the message key expression.
	 * @return the spec.
	 */
	public S messageKeyExpression(Expression messageKeyExpression) {
		this.target.setMessageKeyExpression(messageKeyExpression);
		return _this();
	}

	/**
	 * Configure a {@link Function} that will be invoked at run time to determine the message key under
	 * which a message will be stored in the topic. Typically used with a Java 8 Lambda expression:
	 * <pre class="code">
	 * {@code
	 * .<Foo>messageKey(m -> m.getPayload().getKey())
	 * }
	 * </pre>
	 * @param messageKeyFunction the message key function.
	 * @param <P> the expected payload type.
	 * @return the current {@link KafkaProducerMessageHandlerSpec}.
	 * @see FunctionExpression
	 */
	public <P> S messageKey(Function<Message<P>, ?> messageKeyFunction) {
		return messageKeyExpression(new FunctionExpression<>(messageKeyFunction));
	}

	/**
	 * Configure a partitionId of Kafka topic.
	 * @param partitionId the partitionId to use.
	 * @return the spec.
	 */
	public S partitionId(Integer partitionId) {
		return partitionIdExpression(new ValueExpression<Integer>(partitionId));
	}

	/**
	 * Configure a SpEL expression to determine the topic partitionId at runtime against
	 * request Message as a root object of evaluation context.
	 * @param partitionIdExpression the partitionId expression to use.
	 * @return the spec.
	 */
	public S partitionIdExpression(String partitionIdExpression) {
		return partitionIdExpression(PARSER.parseExpression(partitionIdExpression));
	}

	/**
	 * Configure a {@link Function} that will be invoked at run time to determine the partition id under
	 * which a message will be stored in the topic. Typically used with a Java 8 Lambda expression:
	 * <pre class="code">
	 * {@code
	 * .partitionId(m -> m.getHeaders().get("partitionId", Integer.class))
	 * }
	 * </pre>
	 * @param partitionIdFunction the partitionId function.
	 * @param <P> the expected payload type.
	 * @return the spec.
	 */
	public <P> S partitionId(Function<Message<P>, Integer> partitionIdFunction) {
		return partitionIdExpression(new FunctionExpression<>(partitionIdFunction));
	}

	/**
	 * Configure an {@link Expression} to determine the topic partitionId at runtime against
	 * request Message as a root object of evaluation context.
	 * @param partitionIdExpression the partitionId expression to use.
	 * @return the spec.
	 */
	public S partitionIdExpression(Expression partitionIdExpression) {
		this.target.setPartitionIdExpression(partitionIdExpression);
		return _this();
	}

	/**
	 * Configure a SpEL expression to determine the timestamp at runtime against a
	 * request Message as a root object of evaluation context.
	 * @param timestampExpression the timestamp expression to use.
	 * @return the spec.
	 */
	public S timestampExpression(String timestampExpression) {
		return this.timestampExpression(PARSER.parseExpression(timestampExpression));
	}

	/**
	 * Configure a {@link Function} that will be invoked at run time to determine the Kafka record timestamp
	 * will be stored in the topic. Typically used with a Java 8 Lambda expression:
	 * <pre class="code">
	 * {@code
	 * .timestamp(m -> m.getHeaders().get("mytimestamp_header", Long.class))
	 * }
	 * </pre>
	 * @param timestampFunction the partitionId function.
	 * @param <P> the expected payload type.
	 * @return the spec.
	 */
	public <P> S timestamp(Function<Message<P>, Long> timestampFunction) {
		return timestampExpression(new FunctionExpression<>(timestampFunction));
	}

	/**
	 * Configure an {@link Expression} to determine the timestamp at runtime against a
	 * request Message as a root object of evaluation context.
	 * @param timestampExpression the timestamp expression to use.
	 * @return the spec.
	 */
	public S timestampExpression(Expression timestampExpression) {
		this.target.setTimestampExpression(timestampExpression);
		return _this();
	}


	/**
	 * A {@code boolean} indicating if the {@link KafkaProducerMessageHandler}
	 * should wait for the send operation results or not. Defaults to {@code false}.
	 * In {@code sync} mode a downstream send operation exception will be re-thrown.
	 * @param sync the send mode; async by default.
	 * @return the spec.
	 */
	public S sync(boolean sync) {
		this.target.setSync(sync);
		return _this();
	}

	/**
	 * Specify a timeout in milliseconds how long {@link KafkaProducerMessageHandler}
	 * should wait wait for send operation results. Defaults to 10 seconds.
	 * @param sendTimeout the timeout to wait for result fo send operation.
	 * @return the spec.
	 */
	public S sendTimeout(long sendTimeout) {
		this.target.setSendTimeout(sendTimeout);
		return _this();
	}

	/**
	 * Add a default header mapper to map spring messaging headers to Kafka headers.
	 * @return the spec.
	 */
	public S headerMapper() {
		return headerMapper(new DefaultKafkaHeaderMapper());
	}

	/**
	 * Specify a header mapper to map spring messaging headers to Kafka headers.
	 * @param mapper the mapper.
	 * @return the spec.
	 */
	public S headerMapper(KafkaHeaderMapper mapper) {
		this.target.setHeaderMapper(mapper);
		return _this();
	}

	/**
	 * A {@link KafkaTemplate}-based {@link KafkaProducerMessageHandlerSpec} extension.
	 *
	 * @param <K> the key type.
	 * @param <V> the value type.
	 */
	public static class KafkaProducerMessageHandlerTemplateSpec<K, V> extends KafkaProducerMessageHandlerSpec<K, V, KafkaProducerMessageHandlerTemplateSpec<K, V>>
			implements ComponentsRegistration {

		private final KafkaTemplateSpec<K, V> kafkaTemplateSpec;

		@SuppressWarnings("unchecked")
		KafkaProducerMessageHandlerTemplateSpec(ProducerFactory<K, V> producerFactory) {
			super(new KafkaTemplate<>(producerFactory));
			this.kafkaTemplateSpec = new KafkaTemplateSpec<>((KafkaTemplate<K, V>) this.target.getKafkaTemplate());
		}

		/**
		 * Configure a Kafka Template by invoking the {@link Consumer} callback, with a
		 * {@link KafkaProducerMessageHandlerSpec.KafkaTemplateSpec} argument.
		 * @param configurer the configurer Java 8 Lambda.
		 * @return the spec.
		 */
		public KafkaProducerMessageHandlerTemplateSpec<K, V> configureKafkaTemplate(
				Consumer<KafkaTemplateSpec<K, V>> configurer) {
			Assert.notNull(configurer, "The 'configurer' cannot be null");
			configurer.accept(this.kafkaTemplateSpec);
			return _this();
		}

		@Override
		public Map<Object, String> getComponentsToRegister() {
			return Collections.singletonMap(this.kafkaTemplateSpec.get(), this.kafkaTemplateSpec.getId());
		}

	}

	/**
	 * An {@link IntegrationComponentSpec} implementation for the {@link KafkaTemplate}.
	 *
	 * @param <K> the key type.
	 * @param <V> the value type.
	 */
	public static class KafkaTemplateSpec<K, V>
			extends IntegrationComponentSpec<KafkaTemplateSpec<K, V>, KafkaTemplate<K, V>> {

		KafkaTemplateSpec(KafkaTemplate<K, V> kafkaTemplate) {
			this.target = kafkaTemplate;
		}

		@Override
		public KafkaTemplateSpec<K, V> id(String id) {
			return super.id(id);
		}

		/**
		 /**
		 * Set the default topic for send methods where a topic is not
		 * providing.
		 * @param defaultTopic the topic.
		 * @return the spec
		 */
		public KafkaTemplateSpec<K, V> defaultTopic(String defaultTopic) {
			this.target.setDefaultTopic(defaultTopic);
			return this;
		}

		/**
		 * Set a {@link ProducerListener} which will be invoked when Kafka acknowledges
		 * a send operation. By default a {@link LoggingProducerListener} is configured
		 * which logs errors only.
		 * @param producerListener the listener; may be {@code null}.
		 * @return the spec
		 */
		public KafkaTemplateSpec<K, V> producerListener(ProducerListener<K, V> producerListener) {
			this.target.setProducerListener(producerListener);
			return this;
		}

		/**
		 * Set the message converter to use.
		 * @param messageConverter the message converter.
		 * @return the spec
		 */
		public KafkaTemplateSpec<K, V> messageConverter(RecordMessageConverter messageConverter) {
			this.target.setMessageConverter(messageConverter);
			return this;
		}

	}

}


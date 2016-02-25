/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.kafka.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that marks a method to be the target of a Kafka message
 * listener on the specified topics.
 *
 * TODO: placeholder - needs infrastructure
 *
 * @author Gary Russell
 *  see EnableKafka
 *  see KafkaListenerAnnotationBeanPostProcessor
 * @see KafkaListeners
 */
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
//@MessageMapping
@Documented
@Repeatable(KafkaListeners.class)
public @interface KafkaListener {

	/**
	 * The unique identifier of the container managing for this endpoint.
	 * <p>If none is specified an auto-generated one is provided.
	 * @return the {@code id} for the container managing for this endpoint.
	 *  TODO: see org.springframework.kafka.listener.KafkaListenerEndpointRegistry#getListenerContainer(String)
	 */
	String id() default "";

	/**
	 * The bean name of the {@link org.springframework.kafka.listener.KafkaListenerContainerFactory}
	 * to use to create the message listener container responsible to serve this endpoint.
	 * <p>If not specified, the default container factory is used, if any.
	 * @return the container factory bean name.
	 */
	String containerFactory() default "";

	/**
	 * The topics for this listener.
	 * The entries can be 'topic name', 'property-placeholder keys' or 'expressions'.
	 * Expression must be resolved to the topic name.
	 * Mutually exclusive with {@link #topicPattern()} and {@link #topicPartitions()}.
	 * @return the topic names or expressions (SpEL) to listen to.
	 */
	String[] topics() default {};

	/**
	 * The topic pattern for this listener.
	 * The entries can be 'topic name', 'property-placeholder keys' or 'expressions'.
	 * Expression must be resolved to the topic pattern.
	 * Mutually exclusive with {@link #topics()} and {@link #topicPartitions()}.
	 * @return the topic pattern or expression (SpEL).
	 */
	String topicPattern() default "";

	/**
	 * The topicPartitions for this listener.
	 * Mutually exclusive with {@link #topicPattern()} and {@link #topics()}.
	 * @return the topic names or expressions (SpEL) to listen to.
	 */
	TopicPartition[] topicPartitions() default {};

	/**
	 * If provided, the listener container for this listener will be added to a bean
	 * with this value as its name, of type {@code Collection<MessageListenerContainer>}.
	 * This allows, for example, iteration over the collection to start/stop a subset
	 * of containers.
	 * @return the bean name for the group.
	 * @since 1.5
	 */
	String group() default "";

}

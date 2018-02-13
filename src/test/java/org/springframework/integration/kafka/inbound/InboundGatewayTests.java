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

package org.springframework.integration.kafka.inbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.springframework.kafka.test.assertj.KafkaConditions.partition;
import static org.springframework.kafka.test.assertj.KafkaConditions.value;

import java.lang.reflect.Type;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.handler.advice.ErrorMessageSendingRecoverer;
import org.springframework.integration.kafka.support.RawRecordHeaderErrorMessageStrategy;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.retry.backoff.NoBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/**
 *
 * @author Gary Russell
 *
 * @since 3.0.2
 *
 */
public class InboundGatewayTests {

	private static String topic1 = "testTopic1";

	private static String topic2 = "testTopic2";

	private static String topic3 = "testTopic3";

	private static String topic4 = "testTopic4";

	private static String topic5 = "testTopic5";

	private static String topic6 = "testTopic6";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, topic1, topic2, topic3, topic4, topic5,
			topic6);

	@Test
	public void testInbound() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("test1", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic1);
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic1);
		KafkaInboundGateway<Integer, String, String> gateway = new KafkaInboundGateway<>(container, template);
		QueueChannel out = new QueueChannel();
		DirectChannel reply = new DirectChannel();
		gateway.setRequestChannel(out);
		gateway.setReplyChannel(reply);
		gateway.setBeanFactory(mock(BeanFactory.class));
		gateway.setReplyTimeout(30_000);
		gateway.afterPropertiesSet();
		gateway.setMessageConverter(new MessagingMessageConverter() {

			@Override
			public Message<?> toMessage(ConsumerRecord<?, ?> record, Acknowledgment acknowledgment,
					Consumer<?, ?> consumer, Type type) {
				Message<?> message = super.toMessage(record, acknowledgment, consumer, type);
				return MessageBuilder.fromMessage(message)
						.setHeader("testHeader", "testValue")
						.setHeader(KafkaHeaders.REPLY_TOPIC, topic2)
						.setHeader(KafkaHeaders.REPLY_PARTITION, 1)
						.build();
			}

		});
		gateway.start();
		ContainerTestUtils.waitForAssignment(container, 2);

		template.sendDefault(0, 1487694048607L, 1, "foo");
		Message<?> received = out.receive(30_000);
		assertThat(received).isNotNull();

		MessageHeaders headers = received.getHeaders();
		assertThat(headers.get(KafkaHeaders.RECEIVED_MESSAGE_KEY)).isEqualTo(1);
		assertThat(headers.get(KafkaHeaders.RECEIVED_TOPIC)).isEqualTo(topic1);
		assertThat(headers.get(KafkaHeaders.RECEIVED_PARTITION_ID)).isEqualTo(0);
		assertThat(headers.get(KafkaHeaders.OFFSET)).isEqualTo(0L);
		assertThat(headers.get(KafkaHeaders.RECEIVED_TIMESTAMP)).isEqualTo(1487694048607L);
		assertThat(headers.get(KafkaHeaders.TIMESTAMP_TYPE)).isEqualTo("CREATE_TIME");
		assertThat(headers.get(KafkaHeaders.REPLY_TOPIC)).isEqualTo(topic2);
		assertThat(headers.get("testHeader")).isEqualTo("testValue");
		reply.send(MessageBuilder.withPayload("FOO").copyHeaders(headers).build());

		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("replyHandler1", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		ConsumerFactory<Integer, String> cf2 = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf2.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic2);
		ConsumerRecord<Integer, String> record = KafkaTestUtils.getSingleRecord(consumer, topic2);
		assertThat(record).has(partition(1));
		assertThat(record).has(value("FOO"));

		gateway.stop();
	}

	@Test
	public void testInboundErrorRecover() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("test2", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic3);
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic3);
		KafkaInboundGateway<Integer, String, String> gateway = new KafkaInboundGateway<>(container, template);
		MessageChannel out = new DirectChannel() {

			@Override
			protected boolean doSend(Message<?> message, long timeout) {
				throw new RuntimeException("intended");
			}

		};
		QueueChannel errors = new QueueChannel();
		gateway.setRequestChannel(out);
		gateway.setErrorChannel(errors);
		gateway.setBeanFactory(mock(BeanFactory.class));
		gateway.setMessageConverter(new MessagingMessageConverter() {

			@Override
			public Message<?> toMessage(ConsumerRecord<?, ?> record, Acknowledgment acknowledgment,
					Consumer<?, ?> consumer, Type type) {
				Message<?> message = super.toMessage(record, acknowledgment, consumer, type);
				return MessageBuilder.fromMessage(message)
						.setHeader("testHeader", "testValue")
						.setHeader(KafkaHeaders.REPLY_TOPIC, topic4)
						.setHeader(KafkaHeaders.REPLY_PARTITION, 1)
						.build();
			}

		});
		gateway.setReplyTimeout(30_000);
		gateway.afterPropertiesSet();
		gateway.start();
		ContainerTestUtils.waitForAssignment(container, 2);

		template.sendDefault(0, 1487694048607L, 1, "foo");
		ErrorMessage em = (ErrorMessage) errors.receive(30_000);
		assertThat(em).isNotNull();
		Message<?> failed = ((MessagingException) em.getPayload()).getFailedMessage();
		assertThat(failed).isNotNull();
		MessageChannel reply = (MessageChannel) em.getHeaders().getReplyChannel();
		MessageHeaders headers = failed.getHeaders();
		reply.send(MessageBuilder.withPayload("ERROR").copyHeaders(headers).build());

		assertThat(headers.get(KafkaHeaders.RECEIVED_MESSAGE_KEY)).isEqualTo(1);
		assertThat(headers.get(KafkaHeaders.RECEIVED_TOPIC)).isEqualTo(topic3);
		assertThat(headers.get(KafkaHeaders.RECEIVED_PARTITION_ID)).isEqualTo(0);
		assertThat(headers.get(KafkaHeaders.OFFSET)).isEqualTo(0L);
		assertThat(headers.get(KafkaHeaders.RECEIVED_TIMESTAMP)).isEqualTo(1487694048607L);
		assertThat(headers.get(KafkaHeaders.TIMESTAMP_TYPE)).isEqualTo("CREATE_TIME");
		assertThat(headers.get(KafkaHeaders.REPLY_TOPIC)).isEqualTo(topic4);
		assertThat(headers.get("testHeader")).isEqualTo("testValue");

		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("replyHandler2", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		ConsumerFactory<Integer, String> cf2 = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf2.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic4);
		ConsumerRecord<Integer, String> record = KafkaTestUtils.getSingleRecord(consumer, topic4);
		assertThat(record).has(partition(1));
		assertThat(record).has(value("ERROR"));

		gateway.stop();
	}

	@Test
	public void testInboundRetryErrorRecover() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("test3", "false", embeddedKafka);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(props);
		ContainerProperties containerProps = new ContainerProperties(topic5);
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic5);
		KafkaInboundGateway<Integer, String, String> gateway = new KafkaInboundGateway<>(container, template);
		MessageChannel out = new DirectChannel() {

			@Override
			protected boolean doSend(Message<?> message, long timeout) {
				throw new RuntimeException("intended");
			}

		};
		QueueChannel errors = new QueueChannel();
		gateway.setRequestChannel(out);
		gateway.setErrorChannel(errors);
		gateway.setBeanFactory(mock(BeanFactory.class));
		gateway.setMessageConverter(new MessagingMessageConverter() {

			@Override
			public Message<?> toMessage(ConsumerRecord<?, ?> record, Acknowledgment acknowledgment,
					Consumer<?, ?> consumer, Type type) {
				Message<?> message = super.toMessage(record, acknowledgment, consumer, type);
				return MessageBuilder.fromMessage(message)
						.setHeader("testHeader", "testValue")
						.setHeader(KafkaHeaders.REPLY_TOPIC, topic6)
						.setHeader(KafkaHeaders.REPLY_PARTITION, 1)
						.build();
			}

		});
		gateway.setReplyTimeout(30_000);
		RetryTemplate retryTemplate = new RetryTemplate();
		SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
		retryPolicy.setMaxAttempts(2);
		retryTemplate.setRetryPolicy(retryPolicy);
		retryTemplate.setBackOffPolicy(new NoBackOffPolicy());
		gateway.setRetryTemplate(retryTemplate);
		gateway.setRecoveryCallback(
				new ErrorMessageSendingRecoverer(errors, new RawRecordHeaderErrorMessageStrategy()));
		gateway.afterPropertiesSet();
		gateway.start();
		ContainerTestUtils.waitForAssignment(container, 2);

		template.sendDefault(0, 1487694048607L, 1, "foo");
		ErrorMessage em = (ErrorMessage) errors.receive(30_000);
		assertThat(em).isNotNull();
		Message<?> failed = ((MessagingException) em.getPayload()).getFailedMessage();
		assertThat(failed).isNotNull();
		MessageChannel reply = (MessageChannel) em.getHeaders().getReplyChannel();
		MessageHeaders headers = failed.getHeaders();
		reply.send(MessageBuilder.withPayload("ERROR").copyHeaders(headers).build());

		assertThat(headers.get(KafkaHeaders.RECEIVED_MESSAGE_KEY)).isEqualTo(1);
		assertThat(headers.get(KafkaHeaders.RECEIVED_TOPIC)).isEqualTo(topic5);
		assertThat(headers.get(KafkaHeaders.RECEIVED_PARTITION_ID)).isEqualTo(0);
		assertThat(headers.get(KafkaHeaders.OFFSET)).isEqualTo(0L);
		assertThat(headers.get(KafkaHeaders.RECEIVED_TIMESTAMP)).isEqualTo(1487694048607L);
		assertThat(headers.get(KafkaHeaders.TIMESTAMP_TYPE)).isEqualTo("CREATE_TIME");
		assertThat(headers.get(KafkaHeaders.REPLY_TOPIC)).isEqualTo(topic6);
		assertThat(headers.get("testHeader")).isEqualTo("testValue");

		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("replyHandler3", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		ConsumerFactory<Integer, String> cf2 = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf2.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic6);
		ConsumerRecord<Integer, String> record = KafkaTestUtils.getSingleRecord(consumer, topic6);
		assertThat(record).has(partition(1));
		assertThat(record).has(value("ERROR"));

		gateway.stop();
	}

}

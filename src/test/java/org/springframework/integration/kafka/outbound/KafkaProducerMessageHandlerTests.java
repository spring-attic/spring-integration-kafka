/*
 * Copyright 2016-2019 the original author or authors.
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

package org.springframework.integration.kafka.outbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.springframework.kafka.test.assertj.KafkaConditions.key;
import static org.springframework.kafka.test.assertj.KafkaConditions.partition;
import static org.springframework.kafka.test.assertj.KafkaConditions.timestamp;
import static org.springframework.kafka.test.assertj.KafkaConditions.value;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.expression.ValueExpression;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.support.KafkaSendFailureException;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.TransactionSupport;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

/**
 * @author Gary Russell
 * @author Biju Kunjummen
 * @author Artem Bilan
 *
 * @since 2.0
 */
public class KafkaProducerMessageHandlerTests {

	private static String topic1 = "testTopic1out";

	private static String topic2 = "testTopic2out";

	private static String topic3 = "testTopic3out";

	private static String topic4 = "testTopic4out";

	private static String topic5 = "testTopic5out";

	private static String topic6 = "testTopic6in";

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule =
			new EmbeddedKafkaRule(1, true, topic1, topic2, topic3, topic4, topic5, topic6);

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

	private static Consumer<Integer, String> consumer;

	@BeforeClass
	public static void setUp() {
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(
				KafkaTestUtils.consumerProps("testOut", "true", embeddedKafka));
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromAllEmbeddedTopics(consumer);
	}

	@AfterClass
	public static void tearDown() {
		consumer.close();
	}

	@Test
	public void testOutbound() throws Exception {
		DefaultKafkaProducerFactory<Integer, String> producerFactory = new DefaultKafkaProducerFactory<>(
				KafkaTestUtils.producerProps(embeddedKafka));
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(producerFactory);
		KafkaProducerMessageHandler<Integer, String> handler = new KafkaProducerMessageHandler<>(template);
		handler.setBeanFactory(mock(BeanFactory.class));
		handler.afterPropertiesSet();

		Message<?> message = MessageBuilder.withPayload("foo")
				.setHeader(KafkaHeaders.TOPIC, topic1)
				.setHeader(KafkaHeaders.MESSAGE_KEY, 2)
				.setHeader(KafkaHeaders.PARTITION_ID, 1)
				.build();
		handler.handleMessage(message);

		ConsumerRecord<Integer, String> record = KafkaTestUtils.getSingleRecord(consumer, topic1);
		assertThat(record).has(key(2));
		assertThat(record).has(partition(1));
		assertThat(record).has(value("foo"));

		message = MessageBuilder.withPayload("bar")
				.setHeader(KafkaHeaders.TOPIC, topic1)
				.setHeader(KafkaHeaders.PARTITION_ID, 0)
				.build();
		handler.handleMessage(message);
		record = KafkaTestUtils.getSingleRecord(consumer, topic1);
		assertThat(record).has(key((Integer) null));
		assertThat(record).has(partition(0));
		assertThat(record).has(value("bar"));

		message = MessageBuilder.withPayload("baz")
				.setHeader(KafkaHeaders.TOPIC, topic1)
				.build();
		handler.handleMessage(message);
		record = KafkaTestUtils.getSingleRecord(consumer, topic1);
		assertThat(record).has(key((Integer) null));
		assertThat(record).has(value("baz"));

		handler.setPartitionIdExpression(new SpelExpressionParser().parseExpression("headers['kafka_partitionId']"));

		message = MessageBuilder.withPayload(KafkaNull.INSTANCE)
				.setHeader(KafkaHeaders.TOPIC, topic1)
				.setHeader(KafkaHeaders.MESSAGE_KEY, 2)
				.setHeader(KafkaHeaders.PARTITION_ID, "1")
				.build();
		handler.handleMessage(message);

		record = KafkaTestUtils.getSingleRecord(consumer, topic1);
		assertThat(record).has(key(2));
		assertThat(record).has(partition(1));
		assertThat(record.value()).isNull();

		producerFactory.destroy();
	}

	@Test
	public void testOutboundWithTimestamp() {
		DefaultKafkaProducerFactory<Integer, String> producerFactory = new DefaultKafkaProducerFactory<>(
				KafkaTestUtils.producerProps(embeddedKafka));
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(producerFactory);
		KafkaProducerMessageHandler<Integer, String> handler = new KafkaProducerMessageHandler<>(template);
		handler.setBeanFactory(mock(BeanFactory.class));
		handler.afterPropertiesSet();

		Message<?> message = MessageBuilder.withPayload("foo")
				.setHeader(KafkaHeaders.TOPIC, topic2)
				.setHeader(KafkaHeaders.MESSAGE_KEY, 2)
				.setHeader(KafkaHeaders.PARTITION_ID, 1)
				.setHeader(KafkaHeaders.TIMESTAMP, 1487694048607L)
				.setHeader("baz", "qux")
				.build();
		handler.handleMessage(message);

		ConsumerRecord<Integer, String> record = KafkaTestUtils.getSingleRecord(consumer, topic2);
		assertThat(record).has(key(2));
		assertThat(record).has(partition(1));
		assertThat(record).has(value("foo"));
		assertThat(record).has(timestamp(1487694048607L));
		Map<String, Object> headers = new HashMap<>();
		new DefaultKafkaHeaderMapper().toHeaders(record.headers(), headers);
		assertThat(headers.size()).isEqualTo(1);
		assertThat(headers.get("baz")).isEqualTo("qux");

		producerFactory.destroy();
	}

	@Test
	public void testOutboundWithTimestampExpression() {
		DefaultKafkaProducerFactory<Integer, String> producerFactory = new DefaultKafkaProducerFactory<>(
				KafkaTestUtils.producerProps(embeddedKafka));
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(producerFactory);
		KafkaProducerMessageHandler<Integer, String> handler = new KafkaProducerMessageHandler<>(template);
		handler.setBeanFactory(mock(BeanFactory.class));
		handler.afterPropertiesSet();

		Message<?> message = MessageBuilder.withPayload("foo")
				.setHeader(KafkaHeaders.TOPIC, topic3)
				.setHeader(KafkaHeaders.MESSAGE_KEY, 2)
				.setHeader(KafkaHeaders.PARTITION_ID, 1)
				.build();

		handler.setTimestampExpression(new ValueExpression<>(1487694048633L));

		handler.handleMessage(message);

		ConsumerRecord<Integer, String> record1 = KafkaTestUtils.getSingleRecord(consumer, topic3);
		assertThat(record1).has(key(2));
		assertThat(record1).has(partition(1));
		assertThat(record1).has(value("foo"));
		assertThat(record1).has(timestamp(1487694048633L));

		Long currentTimeMarker = System.currentTimeMillis();
		handler.setTimestampExpression(new FunctionExpression<Message<?>>(m -> System.currentTimeMillis()));

		handler.handleMessage(message);

		ConsumerRecord<Integer, String> record2 = KafkaTestUtils.getSingleRecord(consumer, topic3);
		assertThat(record2).has(key(2));
		assertThat(record2).has(partition(1));
		assertThat(record2).has(value("foo"));
		assertThat(record2.timestamp()).isGreaterThanOrEqualTo(currentTimeMarker);

		producerFactory.destroy();
	}

	@Test
	public void testOutboundWithAsyncResults() {
		DefaultKafkaProducerFactory<Integer, String> producerFactory = new DefaultKafkaProducerFactory<>(
				KafkaTestUtils.producerProps(embeddedKafka));
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(producerFactory);
		KafkaProducerMessageHandler<Integer, String> handler = new KafkaProducerMessageHandler<>(template);
		handler.setBeanFactory(mock(BeanFactory.class));
		PollableChannel successes = new QueueChannel();
		handler.setSendSuccessChannel(successes);
		handler.afterPropertiesSet();

		Message<?> message = MessageBuilder.withPayload("foo")
				.setHeader(KafkaHeaders.TOPIC, topic4)
				.setHeader(KafkaHeaders.MESSAGE_KEY, 2)
				.setHeader(KafkaHeaders.PARTITION_ID, 1)
				.build();
		handler.handleMessage(message);

		ConsumerRecord<Integer, String> record = KafkaTestUtils.getSingleRecord(consumer, topic4);
		assertThat(record).has(key(2));
		assertThat(record).has(partition(1));
		assertThat(record).has(value("foo"));
		Message<?> received = successes.receive(10000);
		assertThat(received).isNotNull();
		assertThat(received.getPayload()).isEqualTo("foo");
		assertThat(received.getHeaders().get(KafkaHeaders.RECORD_METADATA)).isInstanceOf(RecordMetadata.class);

		final RuntimeException fooException = new RuntimeException("foo");

		handler = new KafkaProducerMessageHandler<>(new KafkaTemplate<Integer, String>(producerFactory) {

			@Override
			protected ListenableFuture<SendResult<Integer, String>> doSend(
					ProducerRecord<Integer, String> producerRecord) {
				SettableListenableFuture<SendResult<Integer, String>> future = new SettableListenableFuture<>();
				future.setException(fooException);
				return future;
			}

		});
		PollableChannel failures = new QueueChannel();
		handler.setSendFailureChannel(failures);
		handler.setBeanFactory(mock(BeanFactory.class));
		handler.afterPropertiesSet();
		message = MessageBuilder.withPayload("bar")
				.setHeader(KafkaHeaders.TOPIC, "foo")
				.setHeader(KafkaHeaders.PARTITION_ID, 0)
				.build();
		handler.handleMessage(message);

		received = failures.receive(10000);
		assertThat(received).isNotNull();
		assertThat(received).isInstanceOf(ErrorMessage.class);
		assertThat(((MessagingException) received.getPayload()).getFailedMessage()).isSameAs(message);
		assertThat(((MessagingException) received.getPayload()).getCause()).isSameAs(fooException);
		assertThat(((KafkaSendFailureException) received.getPayload()).getRecord()).isNotNull();

		producerFactory.destroy();
	}

	@Test
	public void testOutboundGateway() throws Exception {
		ConsumerFactory<Integer, String> consumerFactory = new DefaultKafkaConsumerFactory<>(
				KafkaTestUtils.consumerProps(topic5, "false", embeddedKafka));
		ContainerProperties containerProperties = new ContainerProperties(topic6);
		final CountDownLatch assigned = new CountDownLatch(1);
		containerProperties.setConsumerRebalanceListener(new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				// empty
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				assigned.countDown();
			}

		});
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

		DefaultKafkaProducerFactory<Integer, String> producerFactory = new DefaultKafkaProducerFactory<>(
				KafkaTestUtils.producerProps(embeddedKafka));
		ReplyingKafkaTemplate<Integer, String, String> template = new ReplyingKafkaTemplate<>(producerFactory, container);
		template.start();
		assertThat(assigned.await(30, TimeUnit.SECONDS)).isTrue();
		KafkaProducerMessageHandler<Integer, String> handler = new KafkaProducerMessageHandler<>(template);
		handler.setBeanFactory(mock(BeanFactory.class));
		QueueChannel replies = new QueueChannel();
		handler.setOutputChannel(replies);
		handler.afterPropertiesSet();

		Message<?> message = MessageBuilder.withPayload("foo")
				.setHeader(KafkaHeaders.TOPIC, topic5)
				.setHeader(KafkaHeaders.MESSAGE_KEY, 2)
				.setHeader(KafkaHeaders.PARTITION_ID, 1)
				.build();
		handler.handleMessage(message);

		ConsumerRecord<Integer, String> record = KafkaTestUtils.getSingleRecord(consumer, topic5);
		assertThat(record).has(key(2));
		assertThat(record).has(partition(1));
		assertThat(record).has(value("foo"));
		Map<String, Object> headers = new HashMap<>();
		new DefaultKafkaHeaderMapper().toHeaders(record.headers(), headers);
		assertThat(headers.get(KafkaHeaders.REPLY_TOPIC)).isEqualTo(topic6.getBytes());
		ProducerRecord<Integer, String> pr = new ProducerRecord<>(topic6, 0, 1, "FOO", record.headers());
		template.send(pr);
		Message<?> reply = replies.receive(30_000);
		assertThat(reply).isNotNull();
		assertThat(reply.getPayload()).isEqualTo("FOO");
		assertThat(reply.getHeaders().get(KafkaHeaders.TOPIC)).isNull();
		assertThat(reply.getHeaders().get(KafkaHeaders.CORRELATION_ID)).isNull();

		message = MessageBuilder.withPayload("foo")
				.setHeader(KafkaHeaders.TOPIC, topic5)
				.setHeader(KafkaHeaders.MESSAGE_KEY, 2)
				.setHeader(KafkaHeaders.PARTITION_ID, 1)
				.setHeader(KafkaHeaders.REPLY_TOPIC, "bad")
				.build();
		try {
			handler.handleMessage(message);
			fail("Expected exception");
		}
		catch (MessageHandlingException e) {
			assertThat(e.getCause().getMessage())
					.isEqualTo("The reply topic header [bad] does not match any reply container topic: "
							+ "[" + topic6 + "]");
		}
		message = MessageBuilder.withPayload("foo")
				.setHeader(KafkaHeaders.TOPIC, topic5)
				.setHeader(KafkaHeaders.MESSAGE_KEY, 2)
				.setHeader(KafkaHeaders.PARTITION_ID, 1)
				.setHeader(KafkaHeaders.REPLY_PARTITION, 999)
				.build();
		try {
			handler.handleMessage(message);
			fail("Expected exception");
		}
		catch (MessageHandlingException e) {
			assertThat(e.getCause().getMessage())
					.isEqualTo(
							"The reply partition header [999] does not match any reply container partition for topic ["
									+ topic6 + "]: [0, 1]");
		}

		template.stop();
		// discard from the test consumer
		KafkaTestUtils.getSingleRecord(consumer, topic6);

		producerFactory.destroy();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testTransaction() {
		ProducerFactory pf = mock(ProducerFactory.class);
		given(pf.transactionCapable()).willReturn(true);
		Producer producer = mock(Producer.class);
		given(pf.createProducer()).willReturn(producer);
		ListenableFuture future = mock(ListenableFuture.class);
		willReturn(future).given(producer).send(any(ProducerRecord.class), any(Callback.class));
		KafkaTemplate template = new KafkaTemplate(pf);
		KafkaProducerMessageHandler handler = new KafkaProducerMessageHandler(template);
		handler.setTopicExpression(new LiteralExpression("bar"));
		handler.setBeanFactory(mock(BeanFactory.class));
		handler.afterPropertiesSet();
		handler.start();
		handler.handleMessage(new GenericMessage<>("foo"));
		handler.stop();
		InOrder inOrder = inOrder(producer);
		inOrder.verify(producer).beginTransaction();
		inOrder.verify(producer).send(any(ProducerRecord.class), any(Callback.class));
		inOrder.verify(producer).commitTransaction();
		inOrder.verify(producer).flush();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testConsumeAndProduceTransaction() throws Exception {
		Consumer mockConsumer = mock(Consumer.class);
		final TopicPartition topicPartition = new TopicPartition("foo", 0);
		willAnswer(i -> {
			((ConsumerRebalanceListener) i.getArgument(1))
					.onPartitionsAssigned(Collections.singletonList(topicPartition));
			return null;
		}).given(mockConsumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
		ConsumerRecords records = new ConsumerRecords(Collections.singletonMap(topicPartition,
				Collections.singletonList(new ConsumerRecord<>("foo", 0, 0, "key", "value"))));
		final AtomicBoolean done = new AtomicBoolean();
		willAnswer(i -> {
			if (done.compareAndSet(false, true)) {
				return records;
			}
			else {
				Thread.sleep(500);
				return null;
			}
		}).given(mockConsumer).poll(any(Duration.class));
		ConsumerFactory cf = mock(ConsumerFactory.class);
		willReturn(mockConsumer).given(cf).createConsumer("group", "", null, KafkaTestUtils.defaultPropertyOverrides());
		Producer producer = mock(Producer.class);
		final CountDownLatch closeLatch = new CountDownLatch(2);
		willAnswer(i -> {
			closeLatch.countDown();
			return null;
		}).given(producer).close();
		ProducerFactory pf = mock(ProducerFactory.class);
		given(pf.transactionCapable()).willReturn(true);
		final List<String> transactionalIds = new ArrayList<>();
		willAnswer(i -> {
			transactionalIds.add(TransactionSupport.getTransactionIdSuffix());
			return producer;
		}).given(pf).createProducer();
		KafkaTransactionManager tm = new KafkaTransactionManager(pf);
		PlatformTransactionManager ptm = tm;
		ContainerProperties props = new ContainerProperties("foo");
		props.setGroupId("group");
		props.setTransactionManager(ptm);
		final KafkaTemplate template = new KafkaTemplate(pf);
		KafkaMessageListenerContainer container = new KafkaMessageListenerContainer<>(cf, props);
		container.setBeanName("commit");
		KafkaMessageDrivenChannelAdapter inbound = new KafkaMessageDrivenChannelAdapter<>(container);
		DirectChannel channel = new DirectChannel();
		inbound.setOutputChannel(channel);
		KafkaProducerMessageHandler handler = new KafkaProducerMessageHandler(template);
		handler.setMessageKeyExpression(new LiteralExpression("bar"));
		handler.setTopicExpression(new LiteralExpression("topic"));
		channel.subscribe(handler);
		inbound.afterPropertiesSet();
		inbound.start();
		assertThat(closeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		InOrder inOrder = inOrder(producer);
		inOrder.verify(producer).beginTransaction();
		inOrder.verify(producer).sendOffsetsToTransaction(Collections.singletonMap(topicPartition,
				new OffsetAndMetadata(0)), "group");
		inOrder.verify(producer).commitTransaction();
		inOrder.verify(producer).close();
		inOrder.verify(producer).beginTransaction();
		ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
		inOrder.verify(producer).send(captor.capture(), any(Callback.class));
		assertThat(captor.getValue()).isEqualTo(new ProducerRecord("topic", null, "bar", "value"));
		inOrder.verify(producer).sendOffsetsToTransaction(Collections.singletonMap(topicPartition,
				new OffsetAndMetadata(1)), "group");
		inOrder.verify(producer).commitTransaction();
		inOrder.verify(producer).close();
		container.stop();
		verify(pf, times(2)).createProducer();
		verifyNoMoreInteractions(producer);
		assertThat(transactionalIds.get(0)).isEqualTo("group.foo.0");
		assertThat(transactionalIds.get(0)).isEqualTo("group.foo.0");
	}

}

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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;
import org.mockito.InOrder;

import org.springframework.integration.support.AcknowledgmentCallback.Status;
import org.springframework.integration.support.StaticMessageHeaderAccessor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;


/**
 * @author Gary Russell
 * @since 3.0.1
 *
 */
public class MessageSourceTests {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testAck() throws Exception {
		Consumer consumer = mock(Consumer.class);
		TopicPartition topicPartition = new TopicPartition("foo", 0);
		willAnswer(i -> {
			((ConsumerRebalanceListener) i.getArgument(1))
					.onPartitionsAssigned(Collections.singletonList(topicPartition));
			return null;
		}).given(consumer).subscribe(anyCollection(), any(ConsumerRebalanceListener.class));
		AtomicReference<Set<TopicPartition>> paused = new AtomicReference<>(new HashSet<>());
		willAnswer(i -> {
			paused.set(new HashSet<>(i.getArgument(0)));
			return null;
		}).given(consumer).pause(anyCollection());
		willAnswer(i -> paused.get()).given(consumer).paused();
		Map<TopicPartition, List<ConsumerRecord>> records1 = new LinkedHashMap<>();
		records1.put(topicPartition, Arrays.asList(
				new ConsumerRecord("foo", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 0, null, "foo"),
				new ConsumerRecord("foo", 0, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 0, null, "bar")));
		Map<TopicPartition, List<ConsumerRecord>> records2 = new LinkedHashMap<>();
		records2.put(topicPartition, Arrays.asList(
				new ConsumerRecord("foo", 0, 2L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 0, null, "baz"),
				new ConsumerRecord("foo", 0, 3L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 0, null, "qux")));
		ConsumerRecords cr1 = new ConsumerRecords(records1);
		ConsumerRecords cr2 = new ConsumerRecords(records2);
		ConsumerRecords cr3 = new ConsumerRecords(Collections.emptyMap());
		given(consumer.poll(anyLong())).willReturn(cr1, cr2, cr3);
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		given(consumerFactory.createConsumer(isNull(), anyString(), isNull())).willReturn(consumer);
		KafkaMessageSource source = new KafkaMessageSource(consumerFactory, "foo");
		Message<?> received = source.receive();
		StaticMessageHeaderAccessor.getAcknowledgmentCallback(received)
				.acknowledge(Status.ACCEPT);
		received = source.receive();
		StaticMessageHeaderAccessor.getAcknowledgmentCallback(received)
				.acknowledge(Status.ACCEPT);
		received = source.receive();
		StaticMessageHeaderAccessor.getAcknowledgmentCallback(received)
				.acknowledge(Status.ACCEPT);
		received = source.receive();
		StaticMessageHeaderAccessor.getAcknowledgmentCallback(received)
				.acknowledge(Status.ACCEPT);
		received = source.receive();
		assertThat(received).isNull();
		source.destroy();
		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).subscribe(anyCollection(), any(ConsumerRebalanceListener.class));
		inOrder.verify(consumer).paused();
		inOrder.verify(consumer).poll(anyLong());
		inOrder.verify(consumer).pause(anyCollection());
		inOrder.verify(consumer).commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(1L)));
		inOrder.verify(consumer).commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(2L)));
		inOrder.verify(consumer).paused();
		inOrder.verify(consumer).resume(anyCollection());
		inOrder.verify(consumer).poll(anyLong());
		inOrder.verify(consumer).pause(anyCollection());
		inOrder.verify(consumer).commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(3L)));
		inOrder.verify(consumer).commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(4L)));
		inOrder.verify(consumer).paused();
		inOrder.verify(consumer).resume(anyCollection());
		inOrder.verify(consumer).poll(anyLong());
		inOrder.verify(consumer).pause(anyCollection());
		inOrder.verify(consumer).close(30, TimeUnit.SECONDS);
		inOrder.verifyNoMoreInteractions();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testNack() throws Exception {
		Consumer consumer = mock(Consumer.class);
		TopicPartition topicPartition = new TopicPartition("foo", 0);
		willAnswer(i -> {
			((ConsumerRebalanceListener) i.getArgument(1))
					.onPartitionsAssigned(Collections.singletonList(topicPartition));
			return null;
		}).given(consumer).subscribe(anyCollection(), any(ConsumerRebalanceListener.class));
		AtomicReference<Set<TopicPartition>> paused = new AtomicReference<>(new HashSet<>());
		willAnswer(i -> {
			paused.set(new HashSet<>(i.getArgument(0)));
			return null;
		}).given(consumer).pause(anyCollection());
		willAnswer(i -> paused.get()).given(consumer).paused();
		Map<TopicPartition, List<ConsumerRecord>> records1 = new LinkedHashMap<>();
		records1.put(topicPartition, Arrays.asList(
				new ConsumerRecord("foo", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 0, null, "foo"),
				new ConsumerRecord("foo", 0, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 0, null, "bar")));
		ConsumerRecords cr1 = new ConsumerRecords(records1);
		ConsumerRecords cr2 = new ConsumerRecords(Collections.singletonMap(topicPartition, Arrays.asList(
				new ConsumerRecord("foo", 0, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 0, null, "bar"))));
		ConsumerRecords cr3 = new ConsumerRecords(Collections.emptyMap());
		given(consumer.poll(anyLong())).willReturn(cr1, cr1, cr2, cr3);
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		given(consumerFactory.createConsumer(isNull(), anyString(), isNull())).willReturn(consumer);
		KafkaMessageSource source = new KafkaMessageSource(consumerFactory, "foo");
		Message<?> received = source.receive();
		assertThat(received.getHeaders().get(KafkaHeaders.OFFSET)).isEqualTo(0L);
		StaticMessageHeaderAccessor.getAcknowledgmentCallback(received)
				.acknowledge(Status.REQUEUE);
		received = source.receive();
		assertThat(received.getHeaders().get(KafkaHeaders.OFFSET)).isEqualTo(0L);
		StaticMessageHeaderAccessor.getAcknowledgmentCallback(received)
				.acknowledge(Status.ACCEPT);
		received = source.receive();
		assertThat(received.getHeaders().get(KafkaHeaders.OFFSET)).isEqualTo(1L);
		StaticMessageHeaderAccessor.getAcknowledgmentCallback(received)
				.acknowledge(Status.REQUEUE);
		received = source.receive();
		assertThat(received.getHeaders().get(KafkaHeaders.OFFSET)).isEqualTo(1L);
		StaticMessageHeaderAccessor.getAcknowledgmentCallback(received)
				.acknowledge(Status.ACCEPT);
		received = source.receive();
		source.destroy();
		assertThat(received).isNull();
		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).paused();
		inOrder.verify(consumer).poll(anyLong());
		inOrder.verify(consumer).pause(anyCollection());
		inOrder.verify(consumer).seek(topicPartition, 0L);
		inOrder.verify(consumer).paused();
		inOrder.verify(consumer).resume(anyCollection());
		inOrder.verify(consumer).poll(anyLong());
		inOrder.verify(consumer).pause(anyCollection());
		inOrder.verify(consumer).commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(1L)));
		inOrder.verify(consumer).seek(topicPartition, 1L);
		inOrder.verify(consumer).paused();
		inOrder.verify(consumer).resume(anyCollection());
		inOrder.verify(consumer).poll(anyLong());
		inOrder.verify(consumer).pause(anyCollection());
		inOrder.verify(consumer).commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(2L)));
		inOrder.verify(consumer).paused();
		inOrder.verify(consumer).resume(anyCollection());
		inOrder.verify(consumer).poll(anyLong());
		inOrder.verify(consumer).pause(anyCollection());
		inOrder.verify(consumer).close(30, TimeUnit.SECONDS);
		inOrder.verifyNoMoreInteractions();
	}

}

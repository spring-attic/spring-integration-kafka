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

package org.springframework.integration.kafka.dsl

import assertk.assert
import assertk.assertions.contains
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.isSameAs
import assertk.assertions.isTrue
import assertk.catch
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.integration.IntegrationMessageHeaderAccessor
import org.springframework.integration.MessageRejectedException
import org.springframework.integration.channel.QueueChannel
import org.springframework.integration.config.EnableIntegration
import org.springframework.integration.dsl.IntegrationFlow
import org.springframework.integration.dsl.IntegrationFlows
import org.springframework.integration.dsl.Pollers
import org.springframework.integration.expression.ValueExpression
import org.springframework.integration.handler.advice.ErrorMessageSendingRecoverer
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler
import org.springframework.integration.kafka.support.RawRecordHeaderErrorMessageStrategy
import org.springframework.integration.support.MessageBuilder
import org.springframework.integration.test.util.TestUtils
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.GenericMessageListenerContainer
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.DefaultKafkaHeaderMapper
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.test.rule.KafkaEmbedded
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.messaging.Message
import org.springframework.messaging.MessageChannel
import org.springframework.messaging.PollableChannel
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.support.ErrorMessage
import org.springframework.messaging.support.GenericMessage
import org.springframework.retry.support.RetryTemplate
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.stream.Stream

/**
 * @author Artem Bilan
 *
 * @since 3.0.3
 */

@RunWith(SpringRunner::class)
@DirtiesContext
class KafkaDslKotlinTests {

    companion object {

        const val TEST_TOPIC1 = "test-topic1"

        const val TEST_TOPIC2 = "test-topic2"

        const val TEST_TOPIC3 = "test-topic3"

        const val TEST_TOPIC4 = "test-topic4"

        const val TEST_TOPIC5 = "test-topic5"


        @ClassRule
        @JvmField
        var embeddedKafka = KafkaEmbedded(1, true, TEST_TOPIC1, TEST_TOPIC2, TEST_TOPIC3, TEST_TOPIC4, TEST_TOPIC5)
    }


    @Autowired
    @Qualifier("sendToKafkaFlow.input")
    private lateinit var sendToKafkaFlowInput: MessageChannel

    @Autowired
    private lateinit var listeningFromKafkaResults1: PollableChannel

    @Autowired
    private lateinit var listeningFromKafkaResults2: PollableChannel

    @Autowired
    @Qualifier("kafkaProducer1.handler")
    private lateinit var kafkaProducer1: KafkaProducerMessageHandler<*, *>

    @Autowired
    @Qualifier("kafkaProducer2.handler")
    private lateinit var kafkaProducer2: KafkaProducerMessageHandler<*, *>

    @Autowired
    private lateinit var errorChannel: PollableChannel

    @Autowired(required = false)
    @Qualifier("topic1ListenerContainer")
    private lateinit var messageListenerContainer: MessageListenerContainer

    @Autowired(required = false)
    @Qualifier("kafkaTemplate:test-topic1")
    private lateinit var kafkaTemplateTopic1: KafkaTemplate<Any, Any>

    @Autowired(required = false)
    @Qualifier("kafkaTemplate:test-topic2")
    private lateinit var kafkaTemplateTopic2: KafkaTemplate<*, *>

    @Autowired
    private lateinit var mapper: DefaultKafkaHeaderMapper

    @Autowired
    private lateinit var config: ContextConfiguration

    @Autowired
    private lateinit var gate: Gate

    @Test
    fun testKafkaAdapters() {

        val exception = catch { this.sendToKafkaFlowInput.send(GenericMessage("foo")) }
        assert(exception!!.message).isNotNull {
            it.contains("10 is not in the range")
        }

        this.kafkaProducer1.setPartitionIdExpression(ValueExpression(0))
        this.kafkaProducer2.setPartitionIdExpression(ValueExpression(0))
        this.sendToKafkaFlowInput.send(GenericMessage("foo", hashMapOf<String, Any>("foo" to "bar")))

        assert(TestUtils.getPropertyValue(this.kafkaProducer1, "headerMapper")).isSameAs(this.mapper)

        for (i in 0..99) {
            val receive = this.listeningFromKafkaResults1.receive(20000)
            assert(receive).isNotNull()
            assert(receive!!.payload).isEqualTo("FOO")
            val headers = receive.headers
            assert(headers.containsKey(KafkaHeaders.ACKNOWLEDGMENT)).isTrue()
            val acknowledgment = headers.get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment::class.java)
            acknowledgment?.acknowledge()
            assert(headers[KafkaHeaders.RECEIVED_TOPIC]).isEqualTo(TEST_TOPIC1)
            assert(headers[KafkaHeaders.RECEIVED_MESSAGE_KEY]).isEqualTo(i + 1)
            assert(headers[KafkaHeaders.RECEIVED_PARTITION_ID]).isEqualTo(0)
            assert(headers[KafkaHeaders.OFFSET]).isEqualTo(i.toLong())
            assert(headers[KafkaHeaders.TIMESTAMP_TYPE]).isEqualTo("CREATE_TIME")
            assert(headers[KafkaHeaders.RECEIVED_TIMESTAMP]).isEqualTo(1487694048633L)
            assert(headers["foo"]).isEqualTo("bar")
        }

        for (i in 0..99) {
            val receive = this.listeningFromKafkaResults2.receive(20000)
            assert(receive).isNotNull()
            assert(receive!!.payload).isEqualTo("FOO")
            val headers = receive.headers
            assert(headers.containsKey(KafkaHeaders.ACKNOWLEDGMENT)).isTrue()
            val acknowledgment = headers.get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment::class.java)
            acknowledgment?.acknowledge()
            assert(headers[KafkaHeaders.RECEIVED_TOPIC]).isEqualTo(TEST_TOPIC2)
            assert(headers[KafkaHeaders.RECEIVED_MESSAGE_KEY]).isEqualTo(i + 1)
            assert(headers[KafkaHeaders.RECEIVED_PARTITION_ID]).isEqualTo(0)
            assert(headers[KafkaHeaders.OFFSET]).isEqualTo(i.toLong())
            assert(headers[KafkaHeaders.TIMESTAMP_TYPE]).isEqualTo("CREATE_TIME")
            assert(headers[KafkaHeaders.RECEIVED_TIMESTAMP]).isEqualTo(1487694048644L)
        }

        val message = MessageBuilder.withPayload("BAR").setHeader(KafkaHeaders.TOPIC, TEST_TOPIC2).build()

        this.sendToKafkaFlowInput.send(message)

        assert(this.listeningFromKafkaResults1.receive(10)).isNull()

        val error = this.errorChannel.receive(10000)

        assert(error).isNotNull() {
            it.isInstanceOf(ErrorMessage::class.java)
        }

        val payload = error?.payload

        assert(payload).isNotNull() {
            it.isInstanceOf(MessageRejectedException::class.java)
        }

        assert(this.messageListenerContainer).isNotNull()
        assert(this.kafkaTemplateTopic1).isNotNull()
        assert(this.kafkaTemplateTopic2).isNotNull()

        this.kafkaTemplateTopic1.send(TEST_TOPIC3, "foo")
        assert(this.config.sourceFlowLatch.await(10, TimeUnit.SECONDS)).isTrue()
        assert(this.config.fromSource).isEqualTo("foo")
    }

    @Test
    fun testGateways() {
        assert(this.config.replyContainerLatch.await(30, TimeUnit.SECONDS))
        assert(this.gate.exchange(TEST_TOPIC4, "foo")).isEqualTo("FOO")
    }

    @Configuration
    @EnableIntegration
    @EnableKafka
    class ContextConfiguration {

        val sourceFlowLatch = CountDownLatch(1)

        val replyContainerLatch = CountDownLatch(1)

        var fromSource: Any? = null

        @Bean
        fun consumerFactory(): ConsumerFactory<Int, String> {
            val props = KafkaTestUtils.consumerProps("test1", "false", embeddedKafka)
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            return DefaultKafkaConsumerFactory(props)
        }

        @Bean
        fun errorChannel() = QueueChannel()

        @Bean
        fun topic1ListenerFromKafkaFlow() =
                IntegrationFlows.from(
                        Kafka.messageDrivenChannelAdapter<Int, String>(consumerFactory(),
                                KafkaMessageDrivenChannelAdapter.ListenerMode.record, TEST_TOPIC1)
                                .configureListenerContainer({ c ->
                                    c.ackMode(ContainerProperties.AckMode.MANUAL)
                                            .id("topic1ListenerContainer")
                                })
                                .recoveryCallback(ErrorMessageSendingRecoverer(errorChannel(),
                                        RawRecordHeaderErrorMessageStrategy()))
                                .retryTemplate(RetryTemplate())
                                .filterInRetry(true))
                        .filter(Message::class.java, { m -> m.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY, Integer::class.java)!! < 101 },
                                { f -> f.throwExceptionOnRejection(true) })
                        .transform { it: String -> it.toUpperCase() }
                        .channel { c -> c.queue("listeningFromKafkaResults1") }
                        .get()

        @Bean
        fun topic2ListenerFromKafkaFlow() =
                IntegrationFlows.from(
                        Kafka.messageDrivenChannelAdapter<Int, String>(consumerFactory(),
                                KafkaMessageDrivenChannelAdapter.ListenerMode.record, TEST_TOPIC2)
                                .configureListenerContainer({ c -> c.ackMode(ContainerProperties.AckMode.MANUAL) })
                                .recoveryCallback(ErrorMessageSendingRecoverer(errorChannel(),
                                        RawRecordHeaderErrorMessageStrategy()))
                                .retryTemplate(RetryTemplate())
                                .filterInRetry(true))
                        .filter(Message::class.java, { m -> m.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY, Integer::class.java)!! < 101 },
                                { f -> f.throwExceptionOnRejection(true) })
                        .transform { it: String -> it.toUpperCase() }
                        .channel { c -> c.queue("listeningFromKafkaResults2") }
                        .get()

        @Bean
        fun producerFactory() = DefaultKafkaProducerFactory<Int, String>(KafkaTestUtils.producerProps(embeddedKafka))

        @Bean
        fun sendToKafkaFlow() =
                IntegrationFlow { f ->
                    f.split<String>({ p -> Stream.generate { p }.limit(101) }, null)
                            .publishSubscribeChannel { c ->
                                c.subscribe { sf ->
                                    sf.handle(
                                            kafkaMessageHandler(producerFactory(), TEST_TOPIC1)
                                                    .timestampExpression("T(Long).valueOf('1487694048633')"),
                                            { e -> e.id("kafkaProducer1") })
                                }
                                        .subscribe { sf ->
                                            sf.handle(
                                                    kafkaMessageHandler(producerFactory(), TEST_TOPIC2)
                                                            .timestamp<Any> { _ -> 1487694048644L },
                                                    { e -> e.id("kafkaProducer2") })
                                        }
                            }
                }

        @Bean
        fun mapper() = DefaultKafkaHeaderMapper()

        private fun kafkaMessageHandler(producerFactory: ProducerFactory<Int, String>, topic: String) =
                Kafka.outboundChannelAdapter(producerFactory)
                        .messageKey<Any> { m -> m.headers[IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER] }
                        .headerMapper(mapper())
                        .partitionId<Any> { _ -> 10 }
                        .topicExpression("headers[kafka_topic] ?: '$topic'")
                        .configureKafkaTemplate { t -> t.id("kafkaTemplate:$topic") }


        @Bean
        fun sourceFlow() =
                IntegrationFlows
                        .from(Kafka.inboundChannelAdapter(consumerFactory(), TEST_TOPIC3)) { e -> e.poller(Pollers.fixedDelay(100)) }
                        .handle({ p ->
                            this.fromSource = p.getPayload()
                            this.sourceFlowLatch.countDown()
                        })
                        .get()

        @Bean
        fun replyingKafkaTemplate() =
                ReplyingKafkaTemplate(producerFactory(), replyContainer())
                        .also {
                            it.setReplyTimeout(30000)
                        }

        @Bean
        fun outboundGateFlow() =
                IntegrationFlows.from(Gate::class.java)
                        .handle(Kafka.outboundGateway(replyingKafkaTemplate()))
                        .get()

        private fun replyContainer(): GenericMessageListenerContainer<Int, String> {
            val containerProperties = ContainerProperties(TEST_TOPIC5)
            containerProperties.setGroupId("outGate")
            containerProperties.setConsumerRebalanceListener(object : ConsumerRebalanceListener {

                override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
                    // empty
                }

                override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                    this@ContextConfiguration.replyContainerLatch.countDown()
                }

            })
            return KafkaMessageListenerContainer(consumerFactory(), containerProperties)
        }

        @Bean
        fun serverGateway() =
                IntegrationFlows.from(
                        Kafka.inboundGateway(consumerFactory(), containerProperties(), producerFactory()))
                        .transform { it: String -> it.toUpperCase() }
                        .get()

        private fun containerProperties() =
                ContainerProperties(TEST_TOPIC4)
                        .also {
                            it.setGroupId("inGateGroup")
                        }

    }

    interface Gate {

        fun exchange(@Header(KafkaHeaders.TOPIC) topic: String, out: String): String

    }

}

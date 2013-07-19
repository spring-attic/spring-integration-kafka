Spring Integration Kafka Adapter
=================================================


Welcome to the *Spring Integration Kafka adapter*. Apache Kafka is a distributed publish-subscribe messaging system that is designed for handling terra bytes of high throughput
data at constant time. For more information on Kafka and its design goals, please see [Kafka main page](http://kafka.apache.org/)

Spring Integration Kafka adapters are built for Kafka 0.8 and since 0.8 is not backward compatible with any previous versions, Spring Integration will not
support any Kafka versions prior to 0.8. As of this writing, Kafka 0.8 is still WIP, however a beta release is available [here](http://http://kafka.apache.org/downloads.html).

Checking out and building
-----------------------------

Currently Spring Integration Kafka adapter is built against kafka 0.8 that is backed by
Scala 2.9.2.

In order to build the project:

	./gradlew build

In order to install this into your local maven cache:

	./gradlew install

Spring Integration Kafka project currently supports the following two components. Please keep in mind that
this is very early stage in development and do not yet fully make use of all the features that Kafka provides.

* Outbound Channel Adapter
* Inbound Channel Adapter based on the High level consumer API

Outbound Channel Adapter:
--------------------------------------------

The Outbound channel adapter is used to send messages to Kafka. Messages are read from a Spring Integration channel. One can specify this channel in the application context and then wire
this in the application where messages are sent to kafka.

Once a channel is configured, then messages can be sent to Kafka through this channel. Obviously, Spring Integration specific messages are sent to the adapter and then it will
internally get converted into Kafka messages before sending. In the current version of the outbound adapter,
you have to specify a message key and the topic as header values and the message to send as the payload.
Here is an example.

```java
	final MessageChannel channel = ctx.getBean("inputToKafka", MessageChannel.class);

	channel.send(
			MessageBuilder.withPayload(payload)
					.setHeader("messageKey", "key")
					.setHeader("topic", "test").build());
```

This would create a message with a payload. In addition to this, it also creates two header entries as key/value pairs - one for
the message key and another for the topic that this message belongs to.

Here is how kafka outbound channel adapter is configured:

```xml
	<int-kafka:outbound-channel-adapter id="kafkaOutboundChannelAdapter"
										kafka-producer-context-ref="kafkaProducerContext"
										auto-startup="false"
										channel="inputToKafka">
		<int:poller fixed-delay="1000" time-unit="MILLISECONDS" receive-timeout="0" task-executor="taskExecutor"/>
	</int-kafka:outbound-channel-adapter>
```

The key aspect in this configuration is the producer-context-ref. Producer context contains all the producer configuration for all the topics that this adapter is expected to handle.
A channel in which messages are arriving is configured with the adapter and therefore
any message sent to that channel will be handled by this adapter. You can also configure a poller
depending on the
type of the channel used. For example, in the above configuration, we use a queue based channel
and thus a poller is configured with a task executor. If no messages are available in the queue it will timeout immediately because of
the receive-timeout configuration. Then it will poll again with a delay of 1 second.

Producer context is at the heart of the kafka outbound adapter. Here is an example of how it is configured.

```xml
	<int-kafka:producer-context id="kafkaProducerContext">
		<int-kafka:producer-configurations>
			<int-kafka:producer-configuration broker-list="localhost:9092"
					   key-class-type="java.lang.String"
					   value-class-type="java.lang.String"
					   topic="test1"
					   value-encoder="kafkaEncoder"
					   key-encoder="kafkaEncoder"
					   compression-codec="default"/>
			<int-kafka:producer-configuration broker-list="localhost:9092"
					   topic="test2"
					   compression-codec="default"
					   async="true"/>
			<int-kafka:producer-configuration broker-list="localhost:9092"
						topic="regextopic.*"
						compression-codec="default"/>
		</int-kafka:producer-configurations>
	</int-kafka:producer-context>
```

There are a few things going on here. So, lets go one by one. First of all, producer context is simply a holder of, as the name
indicates, a context for the Kafa producer. It contains one ore more producer configurations. Each producer configuration
is ultimately gets translated into a Kafka native producer. Each producer configuration is per topic based right now.
If you go by the above example, there are two producers generated from this configuration - one for topic named
test1 and another for test2. Each producer can take the following:

	broker-list				List of comma separated brokers that this producer connects to
	topic					Topic name or Java regex pattern of topic name
	compression-codec		Compression method to be used. Default is no compression. Supported compression codec are gzip and snappy.
							Anything else would result in no compression
	value-encoder			Serializer to be used for encoding messages.
	key-encoder				Serializer to be used for encoding the partition key
	key-class-type			Type of the key class. This will be ignored if no key-encoder is provided
	value-class-type		Type of the value class. This will be ignored if no value-encoder is provided.
	partitioner				Custom implementation of a Kafka Partitioner interface.
	async					True/False - default is false. Setting this to true would make the Kafka producer to use
							an async producer
	batch-num-messages		Number of messages to batch at the producer. If async is false, then this has no effect.

The value-encoder and key-encoder are referring to other spring beans. They are essentially implementations of an
interface provided by Kafka, the Encoder interface. Similarly, partitioner also refers a Spring bean which implements
the Kafka Partitioner interface.

Here is an example of configuring an encoder.

```xml
	<bean id="kafkaEncoder" class="org.springframework.integration.kafka.serializer.avro.AvroSpecificDatumBackedKafkaEncoder">
		<constructor-arg value="com.company.AvroGeneratedSpecificRecord" />
	</bean>
```

Spring Integration Kafaka adapter provides Apache Avro backed encoders out of the box, as this is a popular choice
for serialization in the big data spectrum. If no encoders are specified as beans, the default encoders provided
by Kafka will be used. On that not, if the encoder is configured only for the message and not for the key, the same encoder
will be used for both. These are standard Kafka behaviors. Spring Integration Kafka adapter does simply enforce those behaviours.
Kafka default encoder expects the data to come as byte arrays and it is a no-op encoder, i.e. it just takes the byte array as it is.
When default encoders are used, there are two ways a message can be sent.
Either, the sender of the message to the channel
can simply put byte arrays as message key and payload. Or, the key and value can be sent as Java Serializable object.
In the latter case, the Kafka adapter will automatically convert them to byte arrays before sending to Kafka broker.
If the encoders are default and the objets sent are not serializalbe, then that would cause an error. By providing explicit encoders
it is totally up to the developer to configure how the objects are serialized. In that case, the objects may or may not implement
the Serializable interface.

A bit more on the Avro support. There are two flavors of Avro encoders provided, one based on the Avro ReflectDatum and the other
based on SpecificDatum. The encoding using reflection is fairly simple as you only have to configure your POJO or other class types
along with the XML. Here is an example.

```xml
	<bean id="kafkaEncoder" class="org.springframework.integration.kafka.serializer.avro.AvroReflectDatumBackedKafkaEncoder">
		<constructor-arg value="java.lang.String" />
	</bean>
```

Reflection based encoding may not be appropriate for large scale systems and Avro's SpecificDatum based encoders can be a better fit. In this case, you can
generate a specific Avro object (a glorified POJO) from a schema definition. The generated object will store the schema as well. In order to
do this, you need to generate the Avro object separately though. There are both maven and gradle plugins available to do code generation
automatically. You have to provide the avdl or avsc files to specify your schema. Once you take care of these steps, you can simply configure
a specific datum based Avro encoder (see the first example above) and pass along the fully qualified class name of the generated Avro object
for which you want to encode instances. The samples project has examples of using both of these encoders.

Encoding String for key and value is a very common use case and Kafka provides a StringEncoder out of the box. It takes a Kafka specific VerifiableProperties object
 along with its
constructor that wraps a regular Java.util.Properties object. The StringEncoder is great when writing a
 direct Java client that talks to Kafka.
However, when using Spring Integration Kafka adapter, it introduces unnecessary steps to create these
properties objects. Therefore, we provide a wrapper class for this same StringEncoder as part of the SI kafka support, which makes
using it from Spring a bit easier. You can inject
any properties to it in the Spring way. Kafka StringEncoder looks at a specific property for the type of encoding scheme used.
In the wrapper bean provided, this property can simply be injected as a value without constructing any other objects. Spring Integration provided StringEncoder is available
in the package org.springframework.integration.kafka.serializer.common.StringEncoder. The avro support for serialization is
also available in a package called avro under serializer.

Inbound Channel Adapter:
--------------------------------------------

The Inbound channel adapter is used to consume messages from Kafka. These messages will be placed into a channel as Spring Integration specific Messages.
Kafka provides two types of consumer API's primarily. One is called the High Level Consumer and the other is the Simple Consumer. High Level consumer is
pretty complex inside. Nonetheless, for the client, using the high level API is straightforward. Although easy to use, High level consumer
does not provide any offset management. So, if you want to rewind and re-fetch messages, it is not possible to do so using the
High Level Consumer API. Offsets are managed by the Zookeeper internally in the High Level Consumer. If your use case does not require any offset management
or re-reading messages from the same consumer, then high level consumer is a perfect fit. Spring Integration Kafka inbound channel adapter
currently supports only the High Level Consumer. Here are the details of configuring one.

```xml
	<int-kafka:inbound-channel-adapter id="kafkaInboundChannelAdapter"
										   kafka-consumer-context-ref="consumerContext"
										   auto-startup="false"
										   channel="inputFromKafka">
			<int:poller fixed-delay="10" time-unit="MILLISECONDS" max-messages-per-poll="5"/>
	</int-kafka:inbound-channel-adapter>
```

Since this inbound channel adapter uses a Polling Channel under the hood, it must be configured with a Poller. A notable difference
between the poller configured with this inbound adapter and other pollers used in Spring Integration is that the receive-timeout specified on this poller
does not have any effect. The reason for this is because of the way Kafka implements iterators on the consumer stream.
It is using a BlockingQueue internally and thus it would wait indefinitely. Instead of interrupting the underlying thread,
we are leveraging a direct Kafka support for consumer time out. It is configured on the consumer context. Everything else
is pretty much the same as in a regular inbound adapter. Any message that it receives will be sent to the channel configured with it.

Inbound Kafka Adapter must specify a kafka-consumer-context-ref element and here is how it is configured:

```xml
   <int-kafka:consumer-context id="consumerContext"
								   consumer-timeout="4000"
								   zookeeper-connect="zookeeperConnect">
		   <int-kafka:consumer-configurations>
			   <int-kafka:consumer-configuration group-id="default"
					   value-decoder="valueDecoder"
					   key-decoder="valueDecoder"
					   max-messages="5000">
				   <int-kafka:topic id="test1" streams="4"/>
				   <int-kafka:topic id="test2" streams="4"/>
			   </int-kafka:consumer-configuration>
			   <int-kafka:consumer-configuration group-id="default3"
						value-decoder="kafkaSpecificDecoder"
						key-decoder="kafkaReflectionDecoder"
						max-messages="10">
				   <int-kafka:topic-filter pattern="regextopic.*" streams="4" exclude="false"/>
			   </int-kafka:consumer-configuration>
		   </int-kafka:consumer-configurations>
   </int-kafka:consumer-context>
```

`consumer-configuration` supports consuming from specific topic using a `topic` child element or from multiple topics matching a topic regex using `topic-filter` child element. `topic-filter` supports both whitelist and blacklist filter based on `exclude` attribute.

Consumer context requires a reference to a zookeeper-connect which dictates all the zookeeper specific configuration details.
Here is how a zookeeper-connect is configured.

```xml
	<int-kafka:zookeeper-connect id="zookeeperConnect" zk-connect="localhost:2181" zk-connection-timeout="6000"
						zk-session-timeout="6000"
						zk-sync-time="2000" />
```

zk-connect attribute is where you would specify the zookeeper connection. All the other attributes get translated into their
zookeeper counter-part attributes by the consumer.

In the above consumer context, you can also specify a consumer-timeout value which would be used to
timeout the consumer in case of no messages to consume.
This timeout would be applicable to all the streams (threads) in the consumer.
The default value for this in Kafka is -1 which would make it wait
indefinitely. However, Sping Integration overrides it to be 5 seconds by default in order to make sure that no
threads are blocking indefinitely in the lifecycle of the application and thereby
giving them a chance to free up any resources or locks that they hold. It is recommended to
override this value so as to meet any specific use case requirements.
By providing a reasonable consumer-timeout on the context and a fixed-delay value on the poller,
this inbound adapter is capable of simulating a message driven behaviour.

consumer context takes consumer-configurations which are at the core of the inbound adapter. It is a group of one or more
consumer-configuration elements which consists of a consumer group dictated by the group-id. Each consumer-configuration
can be configured with one or more kafka-topics.

In the above example provided, we have a single consumer-configuration that consumes messages from two topics each having 4 streams.
 These streams are fundamentally equivalent to the number of partitions that a topic is configured
 with in the producer. For instance, if you configure your topic with
4 partitions, then the maximum number of streams that you may have in the consumer is also 4.
Any more than this would be a no-op.
If you have less number of streams than the available partitions, then messages from
multiple partitions will be sent to available streams.
Therefore, it is a good practice to limit the number of streams for a topic in the consumer
configuration to the number of partitions configured for the topic. There may be situations
in which a partition may be gone during runtime and in that case the stream receiving
data from the partition will simply timeout and whenever this partition comes back,
it would start read data from it again.

Consumer configuration can also be configured with optional decoders for key and value.
The default ones provided by Kafka are basically no-ops and would consume as byte arrays.
If you provide an encoder for key/value in the producer, then it is recommended to provide
corresponding decoders.
As disussed already in the outbound adapter, Spring Integration Kafka adapter gives Apache Avro based data serialization components
out of the box. You can use any serialization component for this purpose as long as you implement the required encoder/decoder interfaces from Kafka.
As with the Avro encoder support, decoders provided also
implement Reflection and Specific datum based de-serialization. Here is how you would configure kafka decoder beans that is Avro backed.

Using Avro Specific support:

```xml
   <bean id="kafkaDecoder" class="org.springframework.integration.kafka.serializer.avro.AvroSpecificDatumBackedKafkaDecoder">
		   <constructor-arg value="com.domain.AvroGeneratedSpecificRecord" />
   </bean>
```

Using Reflection support:

```xml
   <bean id="kafkaDecoder" class="org.springframework.integration.kafka.serializer.avro.AvroReflectDatumBackedKafkaDecoder">
		   <constructor-arg value="java.lang.String" />
   </bean>
```

Another important attribute for the consumer-configuration is the max-messages.
Please note that this is different from the max-messages-per-poll configured on the inbound adapter
element.
There it means the number of times the receive method called on the adapter.
The max-messages on consumer configuration is different. When you use Kafka for ingesting messages,
it usually means an influx of large amount of data constantly. Because of this,
each time a receive is invoked on the adapter, you would basically get a collection of messages.
The maximum number of messages to retrieve for a topic in each execution of the
receive is what configured through the max-messages attribute on the consumer-configuration.
Basically, if the use case is to receive a constant stream of
large number of data, simply specifying a consumer-timeout alone would not be enough.
You would also need to specify the max number of messages to receive.

The type of the payload of the Message returned by the adapter is the following:

```java
Map<String, Map<Integer, List<Object>>>
```

It is a java.util.Map that contains the topic string consumed as the key and another Map as the value.
The inner map's key is the stream (partition) number and value is a list of message payloads.
The reason for this complex return type is
due to the way Kafka orders the messages. In the high level consumer,
all the messages received in a single stream for a single partition
are guaranteed to be in order. For example, if I have a topic named test configured with
4 partitions and I have 4 corresponding streams
in the consumer, then I would receive data in all the consumer streams in the same order
as they were put in the corresponding partitions. This is another reason to set the number of
consumer streams for a topic same
as the number of broker partitions configured for that topic. Lets say that the number of streams
are less than the number of partitions. Then, normally, there is no
guarantee for any order other than just the fact that a single stream will contain messages
from multiple partitions and the messages from a given single partition received will
still be kept contiguously. By that time probably there is no way to find out which set of messages came from which partition.
By providing this complex map that contains the partition information for the topic, we make sure that the order sent by the producer
is preserved even if the number of streams used was less than the number of broker partitions.

A downstream component which receives the data from the inbound adapter can cast the SI payload to the above
Map.

If your use case does not require ordering of messages during consumption, then you can easily pass this
payload to a standard SI transformer and just get a full dump of the actual payload sent by Kafka.

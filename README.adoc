= Spring Integration Adapter for Apache Kafka

image::https://build.spring.io/plugins/servlet/wittified/build-status/INTEXT-KAFKA[]

The *Spring Integration for Apache Kafka* extension project provides inbound and outbound channel adapters and gateways for Apache Kafka.
Apache Kafka is a distributed publish-subscribe messaging system that is designed for high throughput (terabytes of data) and low latency (milliseconds).
For more information on Kafka and its design goals, see the https://kafka.apache.org/[Apache Kafka main page].

NOTE: Starting with Spring Integration 5.4.0, this project has been absorbed by the core one under respective `spring-integration-kafka` module.
The project remain here in maintenance mode until Spring Integration support its `5.3.x` generation.
After that it is going to be archived.

Starting from _version 2.0_ version this project is a complete rewrite based on the new
https://github.com/spring-projects/spring-kafka[spring-kafka] project which uses the pure java "new" `Producer` and
`Consumer` clients provided by Kafka.

== Quick Start

See the
https://github.com/spring-projects/spring-integration-samples/tree/main/basic/kafka[Spring Integration kafka Sample] for a simple Spring Boot application that sends and receives messages.

== Checking out and building

In order to build the project:

    ./gradlew build

In order to install this into your local maven cache:

    ./gradlew install

== Documentation

Documentation for this extension is contained in a chapter of the https://docs.spring.io/spring-integration/docs/current/reference/html/kafka.html#kafka[Spring Integration Reference Manual]

== Migrating from 3.0 to 3.1

Producer record metadata for sends performed on the outbound channel adapter are now sent only to the `successChannel`.
With earlier versions, it was sent to the `outputChannel` if no `successChannel` was provided.

== Contributing

https://help.github.com/en/articles/creating-a-pull-request[Pull requests] are welcome. Please see the https://github.com/spring-projects/spring-integration/blob/main/CONTRIBUTING.adoc[contributor guidelines] for details.

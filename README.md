# Dropwizard ActiveMQ

[![Build](https://github.com/kiwiproject/dropwizard-activemq/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/kiwiproject/dropwizard-activemq/actions/workflows/build.yml?query=branch%3Amain)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=kiwiproject_dropwizard-activemq&metric=alert_status)](https://sonarcloud.io/dashboard?id=kiwiproject_dropwizard-activemq)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=kiwiproject_dropwizard-activemq&metric=coverage)](https://sonarcloud.io/dashboard?id=kiwiproject_dropwizard-activemq)
[![javadoc](https://javadoc.io/badge2/org.kiwiproject/dropwizard-activemq/javadoc.svg)](https://javadoc.io/doc/org.kiwiproject/dropwizard-activemq)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Maven Central](https://img.shields.io/maven-central/v/org.kiwiproject/dropwizard-activemq)](https://central.sonatype.com/artifact/org.kiwiproject/dropwizard-activemq/)

A small library that integrates [Dropwizard](https://www.dropwizard.io/) and [Apache ActiveMQ Classic](https://activemq.apache.org/components/classic/), allowing Dropwizard services to consume and produce JMS messages.

## Requirements

- Java 17 or higher
- Dropwizard 5.x
- ActiveMQ Classic 5.18.x or higher (uses AMQ 6.x client libraries)

## Dependency

```xml
<dependency>
    <groupId>org.kiwiproject</groupId>
    <artifactId>dropwizard-activemq</artifactId>
    <version>[current-version]</version>
</dependency>
```

## Quick Start

### 1. Implement ActiveMqConfigured in your configuration class

```java
public class MyAppConfig extends Configuration implements ActiveMqConfigured {

    @Valid
    @NotNull
    @JsonProperty("activeMq")
    private ActiveMqConfig activeMqConfig = new ActiveMqConfig();

    @Override
    public ActiveMqConfig getActiveMqConfig() {
        return activeMqConfig;
    }

    @Override
    public boolean isElucidationEnabled() {
        return false;
    }
}
```

### 2. Configure in YAML

```yaml
activeMq:
  brokerUri: tcp://localhost:61616
  useSecureActiveMQConnections: false
  useSecureRestConnections: false
  consumers:
    - topic:orders
    - queue:notifications
  producers:
    - topic:orders
  healthConfig:
    jmxUser: admin
    jmxCred: admin
```

### 3. Wire up in your Application

```java
@Override
public void run(MyAppConfig config, Environment environment) {
    var activeMq = DropwizardActiveMq.<MyAppConfig>builder()
            .configuration(config)
            .environment(environment)
            .build();

    activeMq.startConsumers(new OrderConsumer());

    var producer = activeMq.startProducers();
    environment.jersey().register(new OrderResource(producer));
}
```

### 4. Implement a consumer

```java
public class OrderConsumer implements ActiveMqConsumer {

    @Override
    public Result consume(ActiveMqMessage message) {
        var body = requireValidBody(message);
        // process body...
        return Result.CONSUMED;
    }
}
```

### 5. Produce messages

```java
// Send to one destination
producer.produceToDestination("topic:orders", payload);

// Send to a destination and the all-events queue
producer.produceToDestinationAndAllEventsQueue("topic:orders", payload);

// Send a BytesMessage
producer.produceBytesMessage("queue:notifications", bytes);
```

## Destination Types

Destinations are configured as strings with a type prefix:

| Prefix           | Type            | Behavior                                                                                                                                             |
|------------------|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| `topic:foo`      | Virtual topic   | Each consumer group gets its own durable queue. Within a group, messages are load-balanced across instances. Different groups receive independently. |
| `fixedtopic:foo` | Plain JMS topic | Every subscriber receives every message (fan-out). No durability — messages are lost if no subscriber is connected.                                  |
| `queue:foo`      | Regular queue   | Competing consumers — each message is delivered to exactly one consumer.                                                                             |

Virtual topics are the recommended default. When a producer sends to `topic:orders`, ActiveMQ routes
the message to `VirtualTopic.orders`. Each consumer group named `myservice` subscribes to
`Consumer.myservice.VirtualTopic.orders`, getting its own independent durable queue.

## Configuration Reference

### ActiveMqConfig

| Property                                  | Default                     | Description                                                                                                                           |
|-------------------------------------------|-----------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| `brokerUri`                               | `ssl://localhost:61617`     | Full broker URI, including failover and connection options.                                                                           |
| `useSecureActiveMQConnections`            | `true`                      | Connect to the broker over TLS. Requires `tlsConfiguration`.                                                                          |
| `useSecureRestConnections`                | `true`                      | Connect to the Jolokia REST API over TLS. Requires `tlsConfiguration`.                                                                |
| `verifyActiveMQBrokerHostnames`           | `true`                      | Verify the broker's TLS hostname.                                                                                                     |
| `verifyRestConnectionHostnames`           | `true`                      | Verify the Jolokia REST TLS hostname.                                                                                                 |
| `tlsConfiguration`                        | _(via `TlsConfigProvider`)_ | Keystore/truststore settings, required when using secure connections. See [TLS / Secure Connections](#tls--secure-connections).       |
| `consumers`                               | `[]`                        | List of destination strings this service consumes.                                                                                    |
| `consumerReceiveTimeout`                  | `400ms`                     | Maximum time a consumer waits for the next message.                                                                                   |
| `autoRegisterConsumers`                   | `true`                      | Whether all configured `consumers` are automatically registered by `DropwizardActiveMq#startConsumers`.                               |
| `allowMultipleConsumersPerDestination`    | `false`                     | Allow more than one consumer to be registered for the same destination.                                                               |
| `producers`                               | `[]`                        | List of destination strings this service produces to.                                                                                 |
| `defaultProducers`                        | `[]`                        | Default producer destinations; defaults to a list containing the all-events queue if left empty.                                      |
| `allowDynamicDestinations`                | `false`                     | Permit destination strings such as `*:topic://topic-A,topic://topic-B,queue://queue-audit`.                                           |
| `timeToLive`                              | `1 hour`                    | TTL applied to all produced messages. Expired messages move to the DLQ. Set to `0` to disable expiry.                                 |
| `allEventsQueueName`                      | `all_events`                | Bare name of the all-events queue (`queue:<name>`).                                                                                   |
| `jolokiaPort`                             | `8161`                      | Port for the ActiveMQ Jolokia REST API.                                                                                               |
| `registerBrokerHealthCheck`               | `true`                      | Register a health check that verifies broker connectivity.                                                                            |
| `brokerHealthCheckConsumerReceiveTimeout` | `400ms`                     | Maximum time the broker health check's temporary consumer waits for a message.                                                        |
| `enableStatsHealthChecks`                 | `true`                      | Register producer and consumer queue-depth health checks via Jolokia.                                                                 |
| `healthCheckNamePrefix`                   | _(none)_                    | Optional prefix for health check names, useful when connecting to multiple brokers.                                                   |
| `enableElucidation`                       | `false`                     | Enable Elucidation service-dependency reporting. See [Elucidation Integration](#elucidation-integration).                             |
| `destinationNormalizers`                  | `[]`                        | Regex normalizers applied only to destination names reported to Elucidation. See [Elucidation Integration](#elucidation-integration). |

### ActiveMqHealthConfig (nested under `healthConfig`)

| Property                           | Default        | Description                                                                                                                                      |
|------------------------------------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| `jmxUser`                          | _(required)_   | Username for Jolokia Basic Auth.                                                                                                                 |
| `jmxCred`                          | _(required)_   | Password for Jolokia Basic Auth.                                                                                                                 |
| `dlqName`                          | `ActiveMQ.DLQ` | Name of the dead-letter queue.                                                                                                                   |
| `ignoredDestinations`              | `[]`           | Destinations to exclude from stats health checks (use the full prefix form, e.g. `queue:my_queue`).                                              |
| `maxPendingThreshold`              | `100`          | Stats health check reports unhealthy when pending message count reaches or exceeds this value.                                                   |
| `minConsumerThreshold`             | `0`            | Stats health check reports unhealthy when the consumer count is less than or equal to this value, subject to `ignoreEmptyQueuesWithNoConsumers`. |
| `ignoreEmptyQueuesWithNoConsumers` | `true`         | Treat empty queues with no consumers as healthy.                                                                                                 |
| `refreshInterval`                  | `2 minutes`    | How often Jolokia stats are refreshed.                                                                                                           |
| `statsTimeout`                     | `10 seconds`   | Maximum time to wait while retrieving destination statistics from Jolokia.                                                                       |

## Health Checks

`DropwizardActiveMq` registers health checks automatically:

| Name                         | Condition                                                                                                                                                                |
|------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ActiveMQ Producer/Consumer` | Registered when `registerBrokerHealthCheck: true` (the default). Verifies the broker is reachable by sending and receiving a test message.                               |
| `ActiveMQ Producer Stats`    | Registered when `enableStatsHealthChecks: true` (the default) and producers are configured. Reports unhealthy when a producer queue depth exceeds `maxPendingThreshold`. |
| `ActiveMQ Consumer Stats`    | Registered when `enableStatsHealthChecks: true` (the default) and consumers are configured. Reports unhealthy when a consumer queue depth exceeds `maxPendingThreshold`. |
| `consumer-<destination>`     | One per consumer destination. Reports unhealthy if the consumer thread has stopped.                                                                                      |

### Dead-letter queue health check

A `DeadLetterQueueHealthCheck` is available but not registered automatically — register it explicitly
when you want the service to report unhealthy if messages appear in the DLQ:

```java
environment.healthChecks().register("dead-letter-queue",
        new DeadLetterQueueHealthCheck(config.getActiveMqConfig()));
```

### Dead-letter queue inspection task

`InspectDlqTask` is an optional Dropwizard admin task that reports message counts for the ActiveMQ dead-letter queue.
Like the dead-letter queue health check, it is not registered automatically.

```java
var queueInspector = new QueueInspector(activeMqConfiguration, jsonHelper);

environment.lifecycle().manage(queueInspector);
environment.admin().addTask(new InspectDlqTask(queueInspector));
```

By default, the task inspects `ActiveMQ.DLQ`. A custom dead-letter queue name can be supplied to the `InspectDlqTask`
constructor.

**Note:** `QueueInspector` relies on ActiveMQ
[destination advisories](https://activemq.apache.org/components/classic/documentation/advisory-message) and
[`DestinationSource`](https://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/advisory/DestinationSource.html)
to discover queues. Advisories are enabled by default. Because `DestinationSource` learns about destinations through
advisory messages, a queue may briefly be reported as nonexistent after startup.

## TLS / Secure Connections

By default, `useSecureActiveMQConnections` and `useSecureRestConnections` are both `true`, and the
default `brokerUri` uses the SSL port (`ssl://localhost:61617`). To use TLS, provide a
`tlsConfiguration` block with your keystore and truststore paths.

For non-production or local development, disable secure connections:

```yaml
activeMq:
  brokerUri: tcp://localhost:61616
  useSecureActiveMQConnections: false
  useSecureRestConnections: false
```

To use TLS with an explicit keystore and truststore:

```yaml
activeMq:
  tlsConfiguration:
    keyStorePath: /path/to/keystore.jks
    keyStorePassword: changeit
    trustStorePath: /path/to/truststore.jks
    trustStorePassword: changeit
```

If `tlsConfiguration` is omitted, `TlsConfigProvider` (from [`dropwizard-config-providers`](https://github.com/kiwiproject/dropwizard-config-providers))
supplies a default `TlsContextConfiguration` (from [`kiwi`](https://github.com/kiwiproject/kiwi)), populated from
environment variables or system properties.

## Elucidation Integration

[Elucidation](https://github.com/elucidation-project/elucidation) discovers service dependencies by recording
consumer and producer events. It is disabled by default; enable it with:

```yaml
activeMq:
  enableElucidation: true
```

Elucidation tracks dependencies by destination name. If your application routes messages to destinations that
contain dynamic identifiers (for example, a separate topic per user or group), each unique identifier would
otherwise appear as a distinct connection, obscuring the underlying relationship. Use `destinationNormalizers`
to collapse those into a canonical form before they're reported:

```yaml
activeMq:
  enableElucidation: true
  destinationNormalizers:
    - pattern: "(myapp.group).*"
      replacement: "$1.##"
    - pattern: "(myapp.user).*"
      replacement: "$1.##"
```

Each normalizer is a regex `pattern` and `replacement` pair, applied in order, following
[`Matcher#replaceAll`](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/regex/Matcher.html#replaceAll(java.lang.String))
semantics (capturing groups like `$1` are supported). Normalizers only affect how destination names are reported
to Elucidation — they have no effect on actual JMS routing.

## License

MIT License. See [LICENSE](LICENSE).

package org.kiwiproject.dropwizard.activemq.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.kiwiproject.base.KiwiStrings.f;
import static org.kiwiproject.dropwizard.activemq.internal.DestinationIdentifier.evaluateDestinationName;

import org.apache.commons.lang3.Strings;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.kiwiproject.dropwizard.activemq.internal.DestinationIdentifier.DestinationType;

@DisplayName("DestinationIdentifier")
class DestinationIdentifierTest {

    enum ActorType {
        PRODUCER, CONSUMER;

        boolean isProducer() {
            return this == PRODUCER;
        }
    }

    @Nested
    class EvaluateDestinationName {

        @ParameterizedTest
        @CsvSource(textBlock = """
                topic:orders, ''
                '', order-service
                '', ''
                """)
        void shouldThrowIllegalArgument_WhenMissingRequiredArguments(String name, String serviceName) {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> evaluateDestinationName(name, true, serviceName));
        }

        @ParameterizedTest
        @CsvSource(textBlock = """
                fixedtopic:invoices, CONSUMER, billing-service, invoices
                fixedtopic:orders, PRODUCER, billing-service, orders
                fixedtopic:orders, CONSUMER, order-service, orders
                fixedtopic:shipping, PRODUCER, shipping-service, shipping
                """)
        void shouldReturnTopicDestinationInfo(String name,
                                              ActorType actorType,
                                              String serviceName,
                                              String expectedName) {
            var destinationInfo = evaluateDestinationName(name, actorType.isProducer(), serviceName)
                    .orElseThrow();

            assertAll(
                    () -> assertThat(destinationInfo.getType()).isEqualTo(DestinationType.TOPIC),
                    () -> assertThat(destinationInfo.getName()).isEqualTo(expectedName)
            );
        }

        @ParameterizedTest
        @CsvSource(textBlock = """
                topic:invoices, CONSUMER, billing-service
                topic:orders, PRODUCER, billing-service
                topic:orders, CONSUMER, order-service
                topic:shipping, PRODUCER, shipping-service
                """)
        void shouldReturnVirtualTopicDestinationInfo(String name,
                                                     ActorType actorType,
                                                     String serviceName) {

            var destinationInfo = evaluateDestinationName(name, actorType.isProducer(), serviceName)
                    .orElseThrow();

            var bareTopicName = Strings.CS.removeStart(name, "topic:");
            var expectedName = actorType.isProducer() ?
                    "VirtualTopic." + bareTopicName :
                    f("Consumer.{}.VirtualTopic.{}", serviceName, bareTopicName);

            var expectedType = actorType.isProducer() ? DestinationType.TOPIC : DestinationType.QUEUE;

            assertAll(
                    () -> assertThat(destinationInfo.getName()).isEqualTo(expectedName),
                    () -> assertThat(destinationInfo.getType()).isEqualTo(expectedType)
            );
        }

        @ParameterizedTest
        @CsvSource(textBlock = """
                queue:invoices, CONSUMER, billing-service, invoices
                queue:orders, PRODUCER, billing-service, orders
                queue:orders, CONSUMER, order-service, orders
                queue:shipping, PRODUCER, shipping-service, shipping
                """)
        void shouldReturnQueueDestinationInfo(String name,
                                              ActorType actorType,
                                              String serviceName,
                                              String expectedName) {
            var destinationInfo = evaluateDestinationName(name, actorType.isProducer(), serviceName)
                    .orElseThrow();

            assertAll(
                    () -> assertThat(destinationInfo.getType()).isEqualTo(DestinationType.QUEUE),
                    () -> assertThat(destinationInfo.getName()).isEqualTo(expectedName)
            );
        }

        @ParameterizedTest
        @CsvSource(textBlock = """
                '*:topic://topicA,topicB,queue://queue1', CONSUMER, my-service
                '*:topic://topicA,topicB,queue://queue1', PRODUCER, my-service
                '*:topic1,queue://queue1,topic2,topic3,queue://queue2', CONSUMER, test-service
                '*:topic1,queue://queue1,topic2,topic3,queue://queue2', PRODUCER, test-service
                """)
        void shouldReturnDynamicDestinationInfo(String name, ActorType actorType, String serviceName) {
            var destinationInfo = evaluateDestinationName(name, actorType.isProducer(), serviceName)
                    .orElseThrow();

            var bareName = Strings.CS.removeStart(name, "*:");

            assertAll(
                    () -> assertThat(destinationInfo.getType()).isEqualTo(DestinationType.TOPIC),
                    () -> assertThat(destinationInfo.getName()).isEqualTo(bareName)
            );
        }

        @ParameterizedTest
        @CsvSource(textBlock = """
                something:invoices, CONSUMER, billing-service, invoices
                orders, PRODUCER, billing-service, orders
                unknown:orders, CONSUMER, order-service, orders
                foo:shipping, PRODUCER, shipping-service, shipping
                """)
        void shouldReturnEmptyOptional_WhenUnknown(String name, ActorType actorType, String serviceName) {
            assertThat(evaluateDestinationName(name, actorType.isProducer(), serviceName))
                    .isEmpty();
        }
    }
}

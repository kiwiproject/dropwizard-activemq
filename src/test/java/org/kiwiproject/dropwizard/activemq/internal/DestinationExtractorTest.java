package org.kiwiproject.dropwizard.activemq.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.activemq.command.ActiveMQTopic;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

@DisplayName("DestinationExtractor")
class DestinationExtractorTest {

    @Test
    void shouldCreateElucidationDestinations() {
        var destination = "targetDestination";
        var messageType = "messageType";

        assertThat(DestinationExtractor.createElucidationDestination(destination, messageType))
                .isEqualTo("targetDestination::messageType");
    }

    @Test
    void shouldSimplifyDestinationName_WhenGivenActiveMQDestination() {
        var destination = "queue:myQueue";

        var activeMQDestination = new ActiveMQTopic(destination);

        assertThat(DestinationExtractor.simplifyDestination(activeMQDestination))
                .isEqualTo("myQueue");
    }

    @ParameterizedTest(name = "evaulating {0}")
    @MethodSource("queueAndTopicNames")
    void shouldSimplifyDestinationNames(String testCondition, Pair<String, String[]> testData) {
        var originalDestination = testData.getLeft();
        var expectedResults = testData.getRight();

        List<String> results = DestinationExtractor.simplifyDestinations(originalDestination);

        assertThat(results).containsExactly(expectedResults);
    }

    private static Stream<Arguments> queueAndTopicNames() {
        return Stream.of(

                // Single argument entries

                Arguments.of("Bare name", Pair.of("all_events", new String[] { "all_events" })),

                Arguments.of("Prefixed queue name", Pair.of("queue://all_events", new String[] { "all_events" })),

                Arguments.of("Prefixed topic name", Pair.of("topic://all_events", new String[] { "all_events" })),

                Arguments.of("Bare VirtualTopic queue name", Pair.of("Consumer.order-service.VirtualTopic.ORDER_REQUEST", new String[] { "ORDER_REQUEST" })),

                Arguments.of("Prefixed VirtualTopic queue name", Pair.of("queue://Consumer.order-service.VirtualTopic.ORDER_REQUEST", new String[] { "ORDER_REQUEST" })),

                Arguments.of("Bare VirtualTopic name", Pair.of("VirtualTopic.ORDER_REQUEST", new String[] { "ORDER_REQUEST" })),

                Arguments.of("Prefixed VirtualTopic name", Pair.of("queue://VirtualTopic.ORDER_REQUEST", new String[] { "ORDER_REQUEST" })),

                Arguments.of("Bare application.group", Pair.of("application.group.42", new String[] { "application.group.##" })),

                Arguments.of("Prefixed application.group", Pair.of("topic://application.group.42", new String[] { "application.group.##" })),

                Arguments.of("Bare application.user", Pair.of("application.user.84", new String[] { "application.user.##" })),

                Arguments.of("Prefixed application.user", Pair.of("topic://application.user.84", new String[] { "application.user.##" })),

                // Multiple argument entries

                // NOTE:
                // Because we use createTopic to create dynamic destinations on ActiveMQ, "topicA" is not prefixed
                // with "topic:", but "queueA" must be prefixed with "queue:"
                //
                // This is due to ActiveMQ's design in that there is not a generic createDestination() method
                //
                // Also, see DynamicDestinations

                Arguments.of("Multiple entries - sent to ActiveMQ", Pair.of("*:dynamicDestination,topicA,queue:queueA", new String[] { "dynamicDestination", "topicA", "queueA" })),

                Arguments.of("Multiple entries - recevied from ActiveMQ", Pair.of("topic://topicA,queue://queueA", new String[] { "topicA", "queueA" }))
        );
    }
}

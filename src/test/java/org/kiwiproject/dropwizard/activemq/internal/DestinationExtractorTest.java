package org.kiwiproject.dropwizard.activemq.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.activemq.command.ActiveMQTopic;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.kiwiproject.dropwizard.activemq.config.DestinationNormalizerConfig;

import java.util.List;
import java.util.stream.Stream;

@DisplayName("DestinationExtractor")
class DestinationExtractorTest {

    private DestinationExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = DestinationExtractor.withNoNormalizers();
    }

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

        assertThat(extractor.simplifyDestination(activeMQDestination))
                .isEqualTo("myQueue");
    }

    @ParameterizedTest(name = "evaluating {0}")
    @MethodSource("queueAndTopicNames")
    void shouldSimplifyDestinationNames(String testCondition, Pair<String, String[]> testData) {
        var originalDestination = testData.getLeft();
        var expectedResults = testData.getRight();

        List<String> results = extractor.simplifyDestinations(originalDestination);

        assertThat(results).containsExactly(expectedResults);
    }

    @Test
    void shouldApplyConfiguredNormalizers() {
        var groupNormalizer = new DestinationNormalizerConfig();
        groupNormalizer.setPattern("(myapp.group).*");
        groupNormalizer.setReplacement("$1.##");

        var userNormalizer = new DestinationNormalizerConfig();
        userNormalizer.setPattern("(myapp.user).*");
        userNormalizer.setReplacement("$1.##");

        var configured = new DestinationExtractor(List.of(groupNormalizer, userNormalizer));

        assertThat(configured.simplifyDestinations("myapp.group.42")).containsExactly("myapp.group.##");
        assertThat(configured.simplifyDestinations("topic://myapp.group.42")).containsExactly("myapp.group.##");
        assertThat(configured.simplifyDestinations("myapp.user.84")).containsExactly("myapp.user.##");
        assertThat(configured.simplifyDestinations("topic://myapp.user.84")).containsExactly("myapp.user.##");
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

                // Multiple argument entries

                // NOTE:
                // Because we use createTopic to create dynamic destinations on ActiveMQ, "topicA" is not prefixed
                // with "topic:", but "queueA" must be prefixed with "queue:"
                //
                // This is due to ActiveMQ design, in that there is not a generic createDestination() method
                //
                // Also, see DynamicDestinations

                Arguments.of("Multiple entries - sent to ActiveMQ", Pair.of("*:dynamicDestination,topicA,queue:queueA", new String[] { "dynamicDestination", "topicA", "queueA" })),

                Arguments.of("Multiple entries - received from ActiveMQ", Pair.of("topic://topicA,queue://queueA", new String[] { "topicA", "queueA" }))
        );
    }
}

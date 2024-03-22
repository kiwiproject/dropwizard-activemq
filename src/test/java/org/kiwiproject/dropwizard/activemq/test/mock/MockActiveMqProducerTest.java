package org.kiwiproject.dropwizard.activemq.test.mock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

@DisplayName("MockActiveMqProducer")
class MockActiveMqProducerTest {

    private MockActiveMqProducer producer;

    @BeforeEach
    void setUp() {
        producer = new MockActiveMqProducer();
    }

    @Test
    void shouldProduceToAllEvents() {
        produceToAllEventsMessages();

        verifyExpectedNumberOfMessagesSent(producer.history(), 0);
        verifyExpectedNumberOfMessagesSent(producer.allEventsHistory(), 5);
        verifyExpectedNumberOfMessagesSent(producer.bytesHistory(), 0);

        verifyAllEventsHasExpectedResults();
    }

    @Test
    void shouldProduceToDestination() {
        produceToDestinationMessages();

        verifyExpectedNumberOfMessagesSent(producer.history(), 5);
        verifyExpectedNumberOfMessagesSent(producer.allEventsHistory(), 0);
        verifyExpectedNumberOfMessagesSent(producer.bytesHistory(), 0);

        verifyNamedDestinationsHaveExpectedResults();
    }

    @Test
    void shouldProduceToDestination_WithHeaders() {
        produceToDestinationMessagesWithHeaders();

        verifyExpectedNumberOfMessagesSent(producer.history(), 5);
        verifyExpectedNumberOfMessagesSent(producer.allEventsHistory(), 0);
        verifyExpectedNumberOfMessagesSent(producer.bytesHistory(), 0);
        verifyExpectedNumberOfHeadersOnMessage(producer.headersHistory(), 1);

        verifyNamedDestinationsHaveExpectedResults();
    }

    @Test
    void shouldProduceToAllEvents_WithHeaders() {
        produceToDestinationAndAllEventsMessagesWithHeaders();

        verifyExpectedNumberOfMessagesSent(producer.history(), 5);
        verifyExpectedNumberOfMessagesSent(producer.allEventsHistory(), 5);
        verifyExpectedNumberOfMessagesSent(producer.bytesHistory(), 0);
        verifyExpectedNumberOfHeadersOnMessage(producer.headersHistory(), 2);

        verifyNamedDestinationsHaveExpectedResults();
        verifyAllEventsHasExpectedResults();
    }

    @Test
    void shouldProduceToDestination_AndAllEvents() {
        produceToDestinationAndAllEventsMessages();

        verifyExpectedNumberOfMessagesSent(producer.history(), 5);
        verifyExpectedNumberOfMessagesSent(producer.allEventsHistory(), 5);
        verifyExpectedNumberOfMessagesSent(producer.bytesHistory(), 0);

        verifyNamedDestinationsHaveExpectedResults();
        verifyAllEventsHasExpectedResults();
    }

    @Test
    void shouldProduceBytesMessage() {
        produceBytesMessages();

        verifyExpectedNumberOfMessagesSent(producer.history(), 0);
        verifyExpectedNumberOfMessagesSent(producer.allEventsHistory(), 0);
        verifyExpectedNumberOfMessagesSent(producer.bytesHistory(), 5);

        verifyNamedDestinationsHaveExpectedEncodedResults();
    }

    @Test
    void shouldCheckHasProducedToDestination() {
        produceToDestinationAndAllEventsMessages();

        assertAll(
            () -> assertThat(producer.hasProducedToAllEvents()).isTrue(),

            () -> assertThat(producer.hasProducedTo("topic:A")).isTrue(),
            () -> assertThat(producer.lastMessageProducedTo("topic:A")).isEqualTo("message 3 for A"),

            () -> assertThat(producer.hasProducedTo("topic:B")).isTrue(),
            () -> assertThat(producer.lastMessageProducedTo("topic:B")).isEqualTo("message 1 for B"),

            () -> assertThat(producer.hasProducedTo("topic:C")).isTrue(),
            () -> assertThat(producer.lastMessageProducedTo("topic:C")).isEqualTo("message 1 for C"),

            () -> assertThat(producer.hasProducedTo("topic:D")).isFalse(),
            () -> assertThat(producer.hasProducedTo("topic:E")).isFalse()
        );
    }

    @Test
    void shouldCheckHasProducedToAllEvents_WhenEmpty() {
        assertThat(producer.hasProducedToAllEvents()).isFalse();
    }

    @Test
    void shouldThrowException_WhenNoLastMessageProducedTo() {
        assertAll(
            () -> assertThatException().isThrownBy(() -> producer.lastMessageProducedTo("topic:Z")),
            () -> assertThatException().isThrownBy(() -> producer.lastMessageProducedToAllEvents())
        );
    }

    @Test
    void shouldReturnEmptyOptional_WhenNoLastMessageProducedTo() {
        assertAll(
            () -> assertThat(producer.lastMessageProducedToOrEmpty("topic:Z")).isEmpty(),
            () -> assertThat(producer.lastMessageProducedToAllEventsOrEmpty()).isEmpty()
        );
    }

    @Test
    void shouldClearAllMessages() {
        produceToDestinationAndAllEventsMessages();;
        produceBytesMessages();

        verifyExpectedNumberOfMessagesSent(producer.history(), 5);
        verifyExpectedNumberOfMessagesSent(producer.allEventsHistory(), 5);
        verifyExpectedNumberOfMessagesSent(producer.bytesHistory(), 5);

        producer.clear();

        assertAll(
            () -> assertThat(producer.history()).isEmpty(),
            () -> assertThat(producer.allEventsHistory()).isEmpty(),
            () -> assertThat(producer.bytesHistory()).isEmpty()
        );
    }

    @Test
    void shouldClearRegularMessages() {
        produceToDestinationAndAllEventsMessages();;
        produceBytesMessages();

        verifyExpectedNumberOfMessagesSent(producer.history(), 5);
        verifyExpectedNumberOfMessagesSent(producer.allEventsHistory(), 5);
        verifyExpectedNumberOfMessagesSent(producer.bytesHistory(), 5);

        producer.clearMessages();

        assertAll(
            () -> assertThat(producer.history()).isEmpty(),
            () -> assertThat(producer.allEventsHistory()).hasSize(5),
            () -> assertThat(producer.bytesHistory()).hasSize(5)
        );
    }

    @Test
    void shouldClearAllEventsMessages() {
        produceToDestinationAndAllEventsMessages();;
        produceBytesMessages();

        verifyExpectedNumberOfMessagesSent(producer.history(), 5);
        verifyExpectedNumberOfMessagesSent(producer.allEventsHistory(), 5);
        verifyExpectedNumberOfMessagesSent(producer.bytesHistory(), 5);

        producer.clearAllEventsMessages();

        assertAll(
            () -> assertThat(producer.history()).hasSize(5),
            () -> assertThat(producer.allEventsHistory()).isEmpty(),
            () -> assertThat(producer.bytesHistory()).hasSize(5)
        );
    }

    @Test
    void shouldClearBytesMessages() {
        produceToDestinationAndAllEventsMessages();;
        produceBytesMessages();

        verifyExpectedNumberOfMessagesSent(producer.history(), 5);
        verifyExpectedNumberOfMessagesSent(producer.allEventsHistory(), 5);
        verifyExpectedNumberOfMessagesSent(producer.bytesHistory(), 5);

        producer.clearBytesMessages();

        assertAll(
            () -> assertThat(producer.history()).hasSize(5),
            () -> assertThat(producer.allEventsHistory()).hasSize(5),
            () -> assertThat(producer.bytesHistory()).isEmpty()
        );
    }

    private void produceToAllEventsMessages() {
        producer.produceToAllEventsQueue("message 1 for A");
        producer.produceToAllEventsQueue("message 1 for B");
        producer.produceToAllEventsQueue("message 2 for A");
        producer.produceToAllEventsQueue("message 1 for C");
        producer.produceToAllEventsQueue("message 3 for A");
    }

    private void produceToDestinationMessages() {
        producer.produceToDestination("topic:A", "message 1 for A");
        producer.produceToDestination("topic:B", "message 1 for B");
        producer.produceToDestination("topic:A", "message 2 for A");
        producer.produceToDestination("topic:C", "message 1 for C");
        producer.produceToDestination("topic:A", "message 3 for A");
    }

    private void produceToDestinationMessagesWithHeaders() {
        producer.produceToDestinationWithHeaders("topic:A", "message 1 for A", Map.of("firstHeaderKey", "firstHeaderValue"));
        producer.produceToDestinationWithHeaders("topic:B", "message 1 for B", Map.of("firstHeaderKey", "firstHeaderValue"));
        producer.produceToDestinationWithHeaders("topic:A", "message 2 for A", Map.of("firstHeaderKey", "firstHeaderValue"));
        producer.produceToDestinationWithHeaders("topic:C", "message 1 for C", Map.of("firstHeaderKey", "firstHeaderValue"));
        producer.produceToDestinationWithHeaders("topic:A", "message 3 for A", Map.of("firstHeaderKey", "firstHeaderValue"));
    }

    private void produceToDestinationAndAllEventsMessagesWithHeaders() {
        List<Pair<String, String>> topicToMessageList = topicToMessageList();

        topicToMessageList.forEach(topicMessagePair ->
                produceMessageToTopic(topicMessagePair.getLeft(), topicMessagePair.getRight(), Map.of("k1", "v1", "k2", "v2"))
        );
    }

    private List<Pair<String, String>> topicToMessageList() {
        return List.of(
            Pair.of("topic:A", "message 1 for A"),
            Pair.of("topic:B", "message 1 for B"),
            Pair.of("topic:A", "message 2 for A"),
            Pair.of("topic:C", "message 1 for C"),
            Pair.of("topic:A", "message 3 for A")
        );
    }

    private void produceMessageToTopic(String destination, String payload, Map<String, Object> headers) {
        producer.produceToDestinationAndAllEventsWithHeaders(destination, payload, headers);
    }

    private void produceToDestinationAndAllEventsMessages() {
        producer.produceToDestinationAndAllEventsQueue("topic:A", "message 1 for A");
        producer.produceToDestinationAndAllEventsQueue("topic:B", "message 1 for B");
        producer.produceToDestinationAndAllEventsQueue("topic:A", "message 2 for A");
        producer.produceToDestinationAndAllEventsQueue("topic:C", "message 1 for C");
        producer.produceToDestinationAndAllEventsQueue("topic:A", "message 3 for A");
    }

    private void produceBytesMessages() {
        producer.produceBytesMessage("topic:A", "message 1 for A");
        producer.produceBytesMessage("topic:B", "message 1 for B");
        producer.produceBytesMessage("topic:A", "message 2 for A");
        producer.produceBytesMessage("topic:C", "message 1 for C");
        producer.produceBytesMessage("topic:A", "message 3 for A");
    }

    private void verifyAllEventsHasExpectedResults() {
        assertAll(
            () -> assertThat(producer.hasProducedToAllEvents()).isTrue(),
            () -> assertThat(producer.allEventsHistory()).containsExactly(
                    "message 1 for A",
                    "message 1 for B",
                    "message 2 for A",
                    "message 1 for C",
                    "message 3 for A"
            ),
            () -> assertThat(producer.lastMessageProducedToAllEventsOrEmpty()).contains("message 3 for A"),
            () -> assertThat(producer.lastMessageProducedToAllEvents()).isEqualTo("message 3 for A")
        );
    }

    private void verifyNamedDestinationsHaveExpectedResults() {
        assertAll(
            () -> assertThat(producer.hasProducedTo("topic:A")).isTrue(),
            () -> assertThat(producer.history("topic:A")).containsExactly(
                "message 1 for A", "message 2 for A", "message 3 for A"),
            () -> assertThat(producer.lastMessageProducedToOrEmpty("topic:A")).contains("message 3 for A"),
            () -> assertThat(producer.lastMessageProducedTo("topic:A")).isEqualTo("message 3 for A"),

            () -> assertThat(producer.hasProducedTo("topic:B")).isTrue(),
            () -> assertThat(producer.history("topic:B")).containsExactly("message 1 for B"),
            () -> assertThat(producer.lastMessageProducedToOrEmpty("topic:B")).contains("message 1 for B"),
            () -> assertThat(producer.lastMessageProducedTo("topic:B")).isEqualTo("message 1 for B"),

            () -> assertThat(producer.hasProducedTo("topic:C")).isTrue(),
            () -> assertThat(producer.history("topic:C")).containsExactly("message 1 for C"),
            () -> assertThat(producer.lastMessageProducedToOrEmpty("topic:C")).contains("message 1 for C"),
            () -> assertThat(producer.lastMessageProducedTo("topic:C")).isEqualTo("message 1 for C")
        );
    }

    private void verifyNamedDestinationsHaveExpectedEncodedResults() {
        assertAll(
            () -> verifyNamedDestinationsContainsEncodedVersionsOf("topic:A",
                    "message 1 for A", "message 2 for A", "message 3 for A"),

            () -> verifyNamedDestinationsContainsEncodedVersionsOf("topic:B",
                    "message 1 for B"),

            () -> verifyNamedDestinationsContainsEncodedVersionsOf("topic:C",
                    "message 1 for C")
        );
    }

    private void verifyNamedDestinationsContainsEncodedVersionsOf(String destination, String... expectedMessages) {
        var messages = producer.bytesHistory(destination);
        var convertedMessages = messages.stream()
                .map(MockActiveMqProducer::decodeFromBase64ToUTF8String)
                .toList();

        assertThat(convertedMessages).containsExactlyInAnyOrder(expectedMessages);
    }

    private void verifyExpectedNumberOfMessagesSent(List<String> destinationHistory, int expectedCount) {
        assertThat(destinationHistory).hasSize(expectedCount);
    }

    private void verifyExpectedNumberOfHeadersOnMessage(List<Pair<String, Map<String, Object>>> messagesAndHeaders,
                                                        int expecteHeaderCount) {

        messagesAndHeaders.forEach(messageAndHeaders ->
                assertThat(messageAndHeaders.getRight().values()).hasSize(expecteHeaderCount));
    }
}

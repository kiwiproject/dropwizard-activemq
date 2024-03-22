package org.kiwiproject.dropwizard.activemq.test.mock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.kiwiproject.dropwizard.activemq.ActiveMqConsumer;
import org.kiwiproject.dropwizard.activemq.ActiveMqMessage;
import org.kiwiproject.dropwizard.activemq.exception.ActiveMqMessageInvalidMessageTypeException;
import org.kiwiproject.dropwizard.activemq.exception.ActiveMqMessageMissingBodyException;
import org.kiwiproject.dropwizard.activemq.test.util.ActiveMqMessages;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

@DisplayName("MockActiveMqConsumer")
class MockActiveMqConsumerTest {

    private static final String BLANK_MESSAGE_TYPE = "  ";
    private static final String TARGET_MESSAGE_TYPE_1 = "TargetType_1";
    private static final String TARGET_MESSAGE_TYPE_2 = "TargetType_2";
    private static final String PAYLOAD = "hello darkness, my old friend";
    private static final String PAYLOAD_JSON = """
            {
                "payload": "%s"
            }
            """.formatted(PAYLOAD);

    private static final String TARGET_QUEUE_1_NAME = "TargetQueue_1";
    private static final String TARGET_QUEUE_2_NAME = "TargetQueue_2";

    private static final ActiveMQQueue UNNAMED_QUEUE = new ActiveMQQueue();
    private static final ActiveMQQueue TARGET_QUEUE_1 = new ActiveMQQueue(TARGET_QUEUE_1_NAME);
    private static final ActiveMQQueue TARGET_QUEUE_2 = new ActiveMQQueue(TARGET_QUEUE_2_NAME);

    private static final String TARGET_TOPIC_1_NAME = "TargetTopic_1";
    private static final String TARGET_TOPIC_2_NAME = "TargetTopic_2";

    private static final ActiveMQTopic UNNAMED_TOPIC = new ActiveMQTopic();
    private static final ActiveMQTopic TARGET_TOPIC_1 = new ActiveMQTopic(TARGET_TOPIC_1_NAME);
    private static final ActiveMQTopic TARGET_TOPIC_2 = new ActiveMQTopic(TARGET_TOPIC_2_NAME);

    // consume() tests: broken message

    @Test
    void shouldThrowException_WhenMessageHasNoTopicOrQueue() {
        var message = createMessageWithEmptyProperties();

        var consumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertThatIllegalStateException()
                .isThrownBy(() -> consumer.consume(message))
                .withMessage("Somehow you have found a message that was produced without a queue or a topic!");
    }

    @Test
    void shouldThrowException_WhenRequiringBody_AndMessageHasNoBody() {
        var message = createMessageWithoutBodyUsing(TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var consumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .validateBodyIsPresentOrThrowException()
                .buildConsumer();

        assertThatExceptionOfType(ActiveMqMessageMissingBodyException.class)
                .isThrownBy(() -> consumer.consume(message))
                .withMessage("No body attached to ActiveMqMessage");
    }

    @Test
    void shouldThrowException_WhenRequiringMessageType_AndMessageHasNoMessageType() {
        var message = createMessageWithoutMessageTypeUsing(TARGET_QUEUE_1);

        var consumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .validateBodyIsPresentOrThrowException()
                .validateMessageTypeIsPresentOrThrowException()
                .buildConsumer();

        assertThatExceptionOfType(ActiveMqMessageInvalidMessageTypeException.class)
                .isThrownBy(() -> consumer.consume(message))
                .withMessage("No message type present");
    }

    // consume() tests: queue

    @Test
    void shouldThrowException_WhenMessageHasQueueWithNoName() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, UNNAMED_QUEUE);

        var consumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertThatIllegalStateException()
                .isThrownBy(() -> consumer.consume(message))
                .withMessage("Somehow the message thinks it was consumed by a queue, but the queue doesn't have a name");
    }

    @Test
    void shouldIgnoreQueueMessage_WhenDestinationIsNotRegistered() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var consumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_2_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsIgnored(consumer, TARGET_QUEUE_1_NAME, message);
    }

    @Test
    void shouldIgnoreQueueMessage_WhenMessageType_IsBlank() {
        var message = createMessageFrom(BLANK_MESSAGE_TYPE, TARGET_QUEUE_1);

        var consumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsIgnored(consumer, TARGET_QUEUE_1_NAME, message);
    }

    @Test
    void shouldIgnoreQueueMessage_WhenMessageType_IsNotConfigured() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var consumer = MockActiveMqConsumer.builder().buildConsumer();

        assertMessageIsIgnored(consumer, TARGET_QUEUE_1_NAME, message);
    }

    @Test
    void shouldIgnoreQueueMessage_WhenMessageType_IsConfiguredToBeIgnored() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var consumer = MockActiveMqConsumer.builder()
                .ignoringMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsIgnored(consumer, TARGET_QUEUE_1_NAME, message);
    }

    @Test
    void shouldConsumeQueueMessage_WhenMessageType_IsConfigured() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var consumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsConsumed(consumer, TARGET_QUEUE_1_NAME, message);
    }

    @Test
    void shouldConsumeQueueMessage_IfMessageTypeIsConfiguredForConsumptionAndIgnoring() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var consumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .ignoringMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsConsumed(consumer, TARGET_QUEUE_1_NAME, message);
    }

    // consume() tests: topic

    @Test
    void shouldThrowException_WhenMessageHasTopicWithNoName() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, UNNAMED_TOPIC);

        var consumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_TOPIC_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertThatIllegalStateException()
                .isThrownBy(() -> consumer.consume(message))
                .withMessage("Somehow the message thinks it was consumed by a topic, but the topic doesn't have a name");
    }

    @Test
    void shouldIgnoreTopicMessage_WhenDestinationIsNotRegistered() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_1);

        var consumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_TOPIC_2_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsIgnored(consumer, TARGET_TOPIC_1_NAME, message);
    }

    @Test
    void shouldIgnoreTopicMessage_WhenMessageType_IsBlank() {
        var message = createMessageFrom(BLANK_MESSAGE_TYPE, TARGET_TOPIC_1);

        var consumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_TOPIC_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsIgnored(consumer, TARGET_TOPIC_1_NAME, message);
    }

    @Test
    void shouldIgnoreTopicMessage_WhenMessageType_IsNotConfigured() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_1);

        var consumer = MockActiveMqConsumer.builder().buildConsumer();

        assertMessageIsIgnored(consumer, TARGET_TOPIC_1_NAME, message);
    }

    @Test
    void shouldIgnoreTopicMessage_WhenMessageType_IsConfiguredToBeIgnored() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_1);

        var consumer = MockActiveMqConsumer.builder()
                .ignoringMessagesOfType(TARGET_TOPIC_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsIgnored(consumer, TARGET_TOPIC_1_NAME, message);
    }

    @Test
    void shouldConsumeTopicMessage_WhenMessageType_IsConfigured() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_1);

        var consumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_TOPIC_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsConsumed(consumer, TARGET_TOPIC_1_NAME, message);
    }

    @Test
    void shouldConsumeTopicMessage_IfMessageTypeIsConfiguredForConsumptionAndIgnoring() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_1);

        var consumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_TOPIC_1_NAME, TARGET_MESSAGE_TYPE_1)
                .ignoringMessagesOfType(TARGET_TOPIC_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsConsumed(consumer, TARGET_TOPIC_1_NAME, message);
    }

    // multiple messages/destination tests

    @Test
    void shouldConsumeMessages_FromQueuesAndTopics() {
        var q1m1_payload = createJsonPayload("queue", 1, 1);
        var q1m1_message = createMessageFrom(q1m1_payload, TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var t2m1_payload = createJsonPayload("topic", 1, 1);
        var t2m1_message = createMessageFrom(t2m1_payload, TARGET_MESSAGE_TYPE_2, TARGET_TOPIC_2);

        var consumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .consumeMessagesOfType(TARGET_TOPIC_2_NAME, TARGET_MESSAGE_TYPE_2)
                .buildConsumer();

        consumer.consume(q1m1_message);
        consumer.consume(t2m1_message);

        assertAll(
            () -> assertThat(consumer.consumedHistory()).containsOnly(q1m1_message, t2m1_message),
            () -> assertThat(consumer.consumedHistory(TARGET_QUEUE_1_NAME)).containsOnly(q1m1_message),
            () -> assertThat(consumer.consumedHistory(TARGET_QUEUE_2_NAME)).isEmpty(),
            () -> assertThat(consumer.consumedHistory(TARGET_TOPIC_1_NAME)).isEmpty(),
            () -> assertThat(consumer.consumedHistory(TARGET_TOPIC_2_NAME)).containsOnly(t2m1_message),

            () -> assertThat(consumer.ignoredHistory()).isEmpty(),
            () -> assertThat(consumer.ignoredHistory(TARGET_QUEUE_1_NAME)).isEmpty(),
            () -> assertThat(consumer.ignoredHistory(TARGET_QUEUE_2_NAME)).isEmpty(),
            () -> assertThat(consumer.ignoredHistory(TARGET_TOPIC_1_NAME)).isEmpty(),
            () -> assertThat(consumer.ignoredHistory(TARGET_TOPIC_2_NAME)).isEmpty()
        );
    }

    @Test
    void shouldIgnoreMessages_FromQueuesAndTopics() {
        var q1m1_payload = createJsonPayload("queue", 1, 1);
        var q1m1_message = createMessageFrom(q1m1_payload, TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var t2m1_payload = createJsonPayload("topic", 1, 1);
        var t2m1_message = createMessageFrom(t2m1_payload, TARGET_MESSAGE_TYPE_2, TARGET_TOPIC_2);

        var consumer = MockActiveMqConsumer.builder()
                .ignoringMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .ignoringMessagesOfType(TARGET_TOPIC_2_NAME, TARGET_MESSAGE_TYPE_2)
                .buildConsumer();

        consumer.consume(q1m1_message);
        consumer.consume(t2m1_message);

        assertAll(
            () -> assertThat(consumer.consumedHistory()).isEmpty(),
            () -> assertThat(consumer.consumedHistory(TARGET_QUEUE_1_NAME)).isEmpty(),
            () -> assertThat(consumer.consumedHistory(TARGET_QUEUE_2_NAME)).isEmpty(),
            () -> assertThat(consumer.consumedHistory(TARGET_TOPIC_1_NAME)).isEmpty(),
            () -> assertThat(consumer.consumedHistory(TARGET_TOPIC_2_NAME)).isEmpty(),

            () -> assertThat(consumer.ignoredHistory()).containsOnly(q1m1_message, t2m1_message),
            () -> assertThat(consumer.ignoredHistory(TARGET_QUEUE_1_NAME)).containsOnly(q1m1_message),
            () -> assertThat(consumer.ignoredHistory(TARGET_QUEUE_2_NAME)).isEmpty(),
            () -> assertThat(consumer.ignoredHistory(TARGET_TOPIC_1_NAME)).isEmpty(),
            () -> assertThat(consumer.ignoredHistory(TARGET_TOPIC_2_NAME)).containsOnly(t2m1_message)
        );
    }

    @Test
    void shouldConsumeMultipleMessages() {
        // queue messages

        var q1m1_payload = createJsonPayload("queue", 1, 1);
        var q1m1_message = createMessageFrom(q1m1_payload, TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var q1m2_payload = createJsonPayload("queue", 1, 2);
        var q1m2_message = createMessageFrom(q1m2_payload, TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var q2m1_payload = createJsonPayload("queue", 2, 1);
        var q2m1_message = createMessageFrom(q2m1_payload, TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_2);

        var q2m2_payload = createJsonPayload("queue", 2, 2);
        var q2m2_message = createMessageFrom(q2m2_payload, TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_2);

        // topic messages

        var t1m1_payload = createJsonPayload("topic", 1, 1);
        var t1m1_message = createMessageFrom(t1m1_payload, TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_1);

        var t1m2_payload = createJsonPayload("topic", 1, 2);
        var t1m2_message = createMessageFrom(t1m2_payload, TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_1);

        var t2m1_payload = createJsonPayload("topic", 2, 1);
        var t2m1_message = createMessageFrom(t2m1_payload, TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_2);

        var t2m2_payload = createJsonPayload("topic", 2, 2);
        var t2m2_message = createMessageFrom(t2m2_payload, TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_2);

        var consumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .consumeMessagesOfType(TARGET_QUEUE_2_NAME, Set.of(TARGET_MESSAGE_TYPE_1, TARGET_MESSAGE_TYPE_2))
                .ignoringMessagesOfType(TARGET_TOPIC_1_NAME, Set.of(TARGET_MESSAGE_TYPE_1, TARGET_MESSAGE_TYPE_2))
                .ignoringMessagesOfType(TARGET_TOPIC_2_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        consumer.consume(q1m1_message);
        consumer.consume(q1m2_message);
        consumer.consume(q2m1_message);
        consumer.consume(q2m2_message);

        consumer.consume(t1m1_message);
        consumer.consume(t1m2_message);
        consumer.consume(t2m1_message);
        consumer.consume(t2m2_message);

        // Right now we should have consumed the queue messages and ignored the topic ones
        assertThat(consumer.consumedHistory()).containsExactlyInAnyOrder(q1m1_message, q1m2_message, q2m1_message, q2m2_message);
        assertThat(consumer.ignoredHistory()).containsExactlyInAnyOrder(t1m1_message, t1m2_message, t2m1_message, t2m2_message);

        // Consume more messages
        assertMessagesAreConsumed(consumer, TARGET_QUEUE_1_NAME, q1m1_message, q1m2_message);
        assertMessagesAreConsumed(consumer, TARGET_QUEUE_2_NAME, q2m1_message, q2m2_message);

        // Clear consumed messages
        consumer.clearConsumedMessages();
        assertThat(consumer.consumedHistory()).isEmpty();
        assertThat(consumer.ignoredHistory()).hasSize(4);

        // Consume more messages (that should be ignored)
        assertMessagesAreIgnored(consumer, TARGET_TOPIC_1_NAME, t1m1_message, t1m2_message);
        assertMessagesAreIgnored(consumer, TARGET_TOPIC_2_NAME, t2m1_message, t2m2_message);
        assertThat(consumer.ignoredHistory(TARGET_TOPIC_1_NAME)).contains(t1m1_message);
        assertThat(consumer.ignoredHistory(TARGET_TOPIC_2_NAME)).contains(t2m1_message);

        // Clear ignored messages
        consumer.clearIgnoredMessages();
        assertThat(consumer.ignoredHistory()).isEmpty();
    }

    private static String createJsonPayload(String destinationType, int destinationNumber, int messageNumber) {
        return """
            {
                "%s": %d,
                "message": %d
            }
            """.formatted(destinationType, destinationNumber, messageNumber);
    }

    private static ActiveMqMessage createMessageWithEmptyProperties() {
        return ActiveMqMessage.builder()
                .properties(Map.of())
                .build();
    }

    private static ActiveMqMessage createMessageWithoutBodyUsing(String messageType, ActiveMQDestination destination) {
        return createMessageFrom(null, messageType, destination);
    }

    private ActiveMqMessage createMessageWithoutMessageTypeUsing(ActiveMQDestination destination) {
        return createMessageFrom(PAYLOAD_JSON, null, destination);
    }

    private static ActiveMqMessage createMessageFrom(String messageType, ActiveMQDestination destination) {
        return createMessageFrom(PAYLOAD_JSON, messageType, destination);
    }

    private static ActiveMqMessage createMessageFrom(String body, String messageType, ActiveMQDestination destination) {
        return ActiveMqMessages.newJsonActiveMqMessage(body, messageType, createPropertiesMapFrom(destination));
    }

    private static Map<String, Object> createPropertiesMapFrom(ActiveMQDestination destination) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ActiveMqMessage.JMS_DESTINATION, destination);
        return properties;
    }

    private static void assertMessagesAreIgnored(MockActiveMqConsumer consumer,
                                                 String destination,
                                                 ActiveMqMessage... messages) {

        Stream.of(messages).forEach(message -> assertMessageIsIgnored(consumer, destination, message));
    }

    private static void assertMessageIsIgnored(MockActiveMqConsumer consumer,
                                               String destination,
                                               ActiveMqMessage message) {

        assertAll(
            () -> assertThat(consumer.consume(message)).isEqualTo(ActiveMqConsumer.Result.IGNORED),
            () -> assertThat(consumer.consumedHistory(destination)).isEmpty(),
            () -> assertThat(consumer.ignoredHistory(destination)).contains(message)
        );
    }

    private static void assertMessagesAreConsumed(MockActiveMqConsumer consumer,
                                                  String destination,
                                                  ActiveMqMessage... messages) {

        Stream.of(messages).forEach(message -> assertMessageIsConsumed(consumer, destination, message));
    }

    private static void assertMessageIsConsumed(MockActiveMqConsumer consumer,
                                                String destination,
                                                ActiveMqMessage message) {

        assertAll(
            () -> assertThat(consumer.consume(message)).isEqualTo(ActiveMqConsumer.Result.CONSUMED),
            () -> assertThat(consumer.consumedHistory(destination)).contains(message),
            () -> assertThat(consumer.ignoredHistory(destination)).isEmpty()
        );
    }
}

package org.kiwiproject.dropwizard.activemq.testing;

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
import org.kiwiproject.dropwizard.activemq.util.UncheckedJMSException;

import jakarta.jms.JMSException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

@DisplayName("FakeActiveMqConsumer")
class FakeActiveMqConsumerTest {

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

    // shouldConsume() tests

    @Test
    void shouldConsume_ShouldReturnTrue_WhenTheFunctionReturnsTrue() {
        var consumer = FakeActiveMqConsumer.builder()
                .withShouldConsume(activeMqMessage -> true)
                .buildConsumer();

        var result1 = consumer.shouldConsume(createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1));
        assertThat(result1).isTrue();

        var result2 = consumer.shouldConsume(createMessageFrom(TARGET_MESSAGE_TYPE_2, TARGET_QUEUE_2));
        assertThat(result2).isTrue();

        var result3 = consumer.shouldConsume(createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_1));
        assertThat(result3).isTrue();

        assertThat(consumer.getShouldConsumeCount()).isEqualTo(3);
    }

    @Test
    void shouldConsume_ShouldReturnFalse_WhenTheFunctionReturnsFalse() {
        var consumer = FakeActiveMqConsumer.builder()
                .withShouldConsume(activeMqMessage -> false)
                .buildConsumer();

        var result1 = consumer.shouldConsume(createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_1));
        assertThat(result1).isFalse();

        var result2 = consumer.shouldConsume(createMessageFrom(TARGET_MESSAGE_TYPE_2, TARGET_QUEUE_2));
        assertThat(result2).isFalse();

        var result3 = consumer.shouldConsume(createMessageFrom(TARGET_MESSAGE_TYPE_2, UNNAMED_QUEUE));
        assertThat(result3).isFalse();

        var result4 = consumer.shouldConsume(createMessageFrom(TARGET_MESSAGE_TYPE_2, TARGET_TOPIC_2));
        assertThat(result4).isFalse();

        assertThat(consumer.getShouldConsumeCount()).isEqualTo(4);
    }

    @Test
    void shouldConsume_ShouldReturn_TheExpectedValue() {
        Function<ActiveMqMessage, Boolean> processOnlyType1 =
                activeMqMessage -> activeMqMessage.getMessageType().orElse("").equals(TARGET_MESSAGE_TYPE_1);

        var consumer = FakeActiveMqConsumer.builder()
                .withShouldConsume(processOnlyType1)
                .buildConsumer();

        var result1 = consumer.shouldConsume(createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_1));
        assertThat(result1).isTrue();

        var result2 = consumer.shouldConsume(createMessageFrom(TARGET_MESSAGE_TYPE_2, TARGET_QUEUE_2));
        assertThat(result2).isFalse();

        var result3 = consumer.shouldConsume(createMessageFrom(TARGET_MESSAGE_TYPE_2, TARGET_QUEUE_2));
        assertThat(result3).isFalse();

        var result4 = consumer.shouldConsume(createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1));
        assertThat(result4).isTrue();

        var result5 = consumer.shouldConsume(createMessageFrom("RandomType", UNNAMED_QUEUE));
        assertThat(result5).isFalse();

        assertThat(consumer.getShouldConsumeCount()).isEqualTo(5);
    }

    // consume() tests: broken message

    @Test
    void shouldThrowException_WhenMessageHasNoTopicOrQueue() {
        var message = createMessageWithEmptyProperties();

        var consumer = FakeActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertThatIllegalStateException()
                .isThrownBy(() -> consumer.consume(message))
                .withMessage("Somehow you have found a message that was produced without a queue or a topic!");
    }

    @Test
    void shouldThrowException_WhenRequiringBody_AndMessageHasNoBody() {
        var message = createMessageWithoutBodyUsing(TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var consumer = FakeActiveMqConsumer.builder()
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

        var consumer = FakeActiveMqConsumer.builder()
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

        var consumer = FakeActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertThatIllegalStateException()
                .isThrownBy(() -> consumer.consume(message))
                .withMessage("Somehow the message thinks it was consumed by a queue, but the queue doesn't have a name");
    }

    @Test
    void shouldIgnoreQueueMessage_WhenDestinationIsNotRegistered() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var consumer = FakeActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_2_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsIgnored(consumer, TARGET_QUEUE_1_NAME, message);
    }

    @Test
    void shouldIgnoreQueueMessage_WhenMessageType_IsBlank() {
        var message = createMessageFrom(BLANK_MESSAGE_TYPE, TARGET_QUEUE_1);

        var consumer = FakeActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsIgnored(consumer, TARGET_QUEUE_1_NAME, message);
    }

    @Test
    void shouldIgnoreQueueMessage_WhenMessageType_IsNotConfigured() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var consumer = FakeActiveMqConsumer.builder().buildConsumer();

        assertMessageIsIgnored(consumer, TARGET_QUEUE_1_NAME, message);
    }

    @Test
    void shouldIgnoreQueueMessage_WhenMessageType_IsConfiguredToBeIgnored() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var consumer = FakeActiveMqConsumer.builder()
                .ignoringMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsIgnored(consumer, TARGET_QUEUE_1_NAME, message);
    }

    @Test
    void shouldConsumeQueueMessage_WhenMessageType_IsConfigured() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var consumer = FakeActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsConsumed(consumer, TARGET_QUEUE_1_NAME, message);
    }

    @Test
    void shouldConsumeQueueMessage_IfMessageTypeIsConfiguredForConsumptionAndIgnoring() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var consumer = FakeActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .ignoringMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsConsumed(consumer, TARGET_QUEUE_1_NAME, message);
    }

    // consume() tests: topic

    @Test
    void shouldThrowException_WhenMessageHasTopicWithNoName() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, UNNAMED_TOPIC);

        var consumer = FakeActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_TOPIC_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertThatIllegalStateException()
                .isThrownBy(() -> consumer.consume(message))
                .withMessage("Somehow the message thinks it was consumed by a topic, but the topic doesn't have a name");
    }

    @Test
    void shouldIgnoreTopicMessage_WhenDestinationIsNotRegistered() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_1);

        var consumer = FakeActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_TOPIC_2_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsIgnored(consumer, TARGET_TOPIC_1_NAME, message);
    }

    @Test
    void shouldIgnoreTopicMessage_WhenMessageType_IsBlank() {
        var message = createMessageFrom(BLANK_MESSAGE_TYPE, TARGET_TOPIC_1);

        var consumer = FakeActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_TOPIC_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsIgnored(consumer, TARGET_TOPIC_1_NAME, message);
    }

    @Test
    void shouldIgnoreTopicMessage_WhenMessageType_IsNotConfigured() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_1);

        var consumer = FakeActiveMqConsumer.builder().buildConsumer();

        assertMessageIsIgnored(consumer, TARGET_TOPIC_1_NAME, message);
    }

    @Test
    void shouldIgnoreTopicMessage_WhenMessageType_IsConfiguredToBeIgnored() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_1);

        var consumer = FakeActiveMqConsumer.builder()
                .ignoringMessagesOfType(TARGET_TOPIC_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsIgnored(consumer, TARGET_TOPIC_1_NAME, message);
    }

    @Test
    void shouldConsumeTopicMessage_WhenMessageType_IsConfigured() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_1);

        var consumer = FakeActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_TOPIC_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsConsumed(consumer, TARGET_TOPIC_1_NAME, message);
    }

    @Test
    void shouldConsumeTopicMessage_IfMessageTypeIsConfiguredForConsumptionAndIgnoring() {
        var message = createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_1);

        var consumer = FakeActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_TOPIC_1_NAME, TARGET_MESSAGE_TYPE_1)
                .ignoringMessagesOfType(TARGET_TOPIC_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertMessageIsConsumed(consumer, TARGET_TOPIC_1_NAME, message);
    }

    // multiple messages/destination tests

    @Test
    void shouldConsumeMessages_FromQueuesAndTopics() {
        var q1m1Payload = createJsonPayload("queue", 1, 1);
        var q1m1Message = createMessageFrom(q1m1Payload, TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var t2m1Payload = createJsonPayload("topic", 1, 1);
        var t2m1Message = createMessageFrom(t2m1Payload, TARGET_MESSAGE_TYPE_2, TARGET_TOPIC_2);

        var consumer = FakeActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .consumeMessagesOfType(TARGET_TOPIC_2_NAME, TARGET_MESSAGE_TYPE_2)
                .buildConsumer();

        consumer.consume(q1m1Message);
        consumer.consume(t2m1Message);

        assertAll(
                () -> assertThat(consumer.consumedHistory()).containsOnly(q1m1Message, t2m1Message),
                () -> assertThat(consumer.consumedHistory(TARGET_QUEUE_1_NAME)).containsOnly(q1m1Message),
                () -> assertThat(consumer.consumedHistory(TARGET_QUEUE_2_NAME)).isEmpty(),
                () -> assertThat(consumer.consumedHistory(TARGET_TOPIC_1_NAME)).isEmpty(),
                () -> assertThat(consumer.consumedHistory(TARGET_TOPIC_2_NAME)).containsOnly(t2m1Message),

                () -> assertThat(consumer.ignoredHistory()).isEmpty(),
                () -> assertThat(consumer.ignoredHistory(TARGET_QUEUE_1_NAME)).isEmpty(),
                () -> assertThat(consumer.ignoredHistory(TARGET_QUEUE_2_NAME)).isEmpty(),
                () -> assertThat(consumer.ignoredHistory(TARGET_TOPIC_1_NAME)).isEmpty(),
                () -> assertThat(consumer.ignoredHistory(TARGET_TOPIC_2_NAME)).isEmpty()
        );
    }

    @Test
    void shouldIgnoreMessages_FromQueuesAndTopics() {
        var q1m1Payload = createJsonPayload("queue", 1, 1);
        var q1m1Message = createMessageFrom(q1m1Payload, TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var t2m1Payload = createJsonPayload("topic", 1, 1);
        var t2m1Message = createMessageFrom(t2m1Payload, TARGET_MESSAGE_TYPE_2, TARGET_TOPIC_2);

        var consumer = FakeActiveMqConsumer.builder()
                .ignoringMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .ignoringMessagesOfType(TARGET_TOPIC_2_NAME, TARGET_MESSAGE_TYPE_2)
                .buildConsumer();

        consumer.consume(q1m1Message);
        consumer.consume(t2m1Message);

        assertAll(
                () -> assertThat(consumer.consumedHistory()).isEmpty(),
                () -> assertThat(consumer.consumedHistory(TARGET_QUEUE_1_NAME)).isEmpty(),
                () -> assertThat(consumer.consumedHistory(TARGET_QUEUE_2_NAME)).isEmpty(),
                () -> assertThat(consumer.consumedHistory(TARGET_TOPIC_1_NAME)).isEmpty(),
                () -> assertThat(consumer.consumedHistory(TARGET_TOPIC_2_NAME)).isEmpty(),

                () -> assertThat(consumer.ignoredHistory()).containsOnly(q1m1Message, t2m1Message),
                () -> assertThat(consumer.ignoredHistory(TARGET_QUEUE_1_NAME)).containsOnly(q1m1Message),
                () -> assertThat(consumer.ignoredHistory(TARGET_QUEUE_2_NAME)).isEmpty(),
                () -> assertThat(consumer.ignoredHistory(TARGET_TOPIC_1_NAME)).isEmpty(),
                () -> assertThat(consumer.ignoredHistory(TARGET_TOPIC_2_NAME)).containsOnly(t2m1Message)
        );
    }

    @Test
    void shouldConsumeMultipleMessages() {
        // queue messages

        var q1m1Payload = createJsonPayload("queue", 1, 1);
        var q1m1Message = createMessageFrom(q1m1Payload, TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var q1m2Payload = createJsonPayload("queue", 1, 2);
        var q1m2Message = createMessageFrom(q1m2Payload, TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1);

        var q2m1Payload = createJsonPayload("queue", 2, 1);
        var q2m1Message = createMessageFrom(q2m1Payload, TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_2);

        var q2m2Payload = createJsonPayload("queue", 2, 2);
        var q2m2Message = createMessageFrom(q2m2Payload, TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_2);

        // topic messages

        var t1m1Payload = createJsonPayload("topic", 1, 1);
        var t1m1Message = createMessageFrom(t1m1Payload, TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_1);

        var t1m2Payload = createJsonPayload("topic", 1, 2);
        var t1m2Message = createMessageFrom(t1m2Payload, TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_1);

        var t2m1Payload = createJsonPayload("topic", 2, 1);
        var t2m1Message = createMessageFrom(t2m1Payload, TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_2);

        var t2m2Payload = createJsonPayload("topic", 2, 2);
        var t2m2Message = createMessageFrom(t2m2Payload, TARGET_MESSAGE_TYPE_1, TARGET_TOPIC_2);

        var consumer = FakeActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .consumeMessagesOfType(TARGET_QUEUE_2_NAME, Set.of(TARGET_MESSAGE_TYPE_1, TARGET_MESSAGE_TYPE_2))
                .ignoringMessagesOfType(TARGET_TOPIC_1_NAME, Set.of(TARGET_MESSAGE_TYPE_1, TARGET_MESSAGE_TYPE_2))
                .ignoringMessagesOfType(TARGET_TOPIC_2_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        consumer.consume(q1m1Message);
        consumer.consume(q1m2Message);
        consumer.consume(q2m1Message);
        consumer.consume(q2m2Message);

        consumer.consume(t1m1Message);
        consumer.consume(t1m2Message);
        consumer.consume(t2m1Message);
        consumer.consume(t2m2Message);

        // Right now we should have consumed the queue messages and ignored the topic ones
        assertThat(consumer.consumedHistory()).containsExactlyInAnyOrder(q1m1Message, q1m2Message, q2m1Message, q2m2Message);
        assertThat(consumer.ignoredHistory()).containsExactlyInAnyOrder(t1m1Message, t1m2Message, t2m1Message, t2m2Message);

        // Consume more messages
        assertMessagesAreConsumed(consumer, TARGET_QUEUE_1_NAME, q1m1Message, q1m2Message);
        assertMessagesAreConsumed(consumer, TARGET_QUEUE_2_NAME, q2m1Message, q2m2Message);

        // Clear consumed messages
        consumer.clearConsumedMessages();
        assertThat(consumer.consumedHistory()).isEmpty();
        assertThat(consumer.ignoredHistory()).hasSize(4);

        // Consume more messages (that should be ignored)
        assertMessagesAreIgnored(consumer, TARGET_TOPIC_1_NAME, t1m1Message, t1m2Message);
        assertMessagesAreIgnored(consumer, TARGET_TOPIC_2_NAME, t2m1Message, t2m2Message);
        assertThat(consumer.ignoredHistory(TARGET_TOPIC_1_NAME)).contains(t1m1Message);
        assertThat(consumer.ignoredHistory(TARGET_TOPIC_2_NAME)).contains(t2m1Message);

        // Clear ignored messages
        consumer.clearIgnoredMessages();
        assertThat(consumer.ignoredHistory()).isEmpty();
    }

    @Test
    void shouldClearBothHistories_WhenClearCalled() {
        var consumer = FakeActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .ignoringMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_2)
                .buildConsumer();

        consumer.consume(createMessageFrom(TARGET_MESSAGE_TYPE_1, TARGET_QUEUE_1));
        consumer.consume(createMessageFrom(TARGET_MESSAGE_TYPE_2, TARGET_QUEUE_1));

        assertThat(consumer.consumedHistory()).hasSize(1);
        assertThat(consumer.ignoredHistory()).hasSize(1);

        consumer.clear();

        assertAll(
                () -> assertThat(consumer.consumedHistory()).isEmpty(),
                () -> assertThat(consumer.ignoredHistory()).isEmpty()
        );
    }

    @Test
    void shouldThrowUncheckedJMSException_WhenQueueGetNameThrowsJMSException() {
        var throwingQueue = new ActiveMQQueue() {
            @Override
            public String getQueueName() throws JMSException {
                throw new JMSException("queue name failure");
            }
        };

        Map<String, Object> properties = new HashMap<>();
        properties.put(ActiveMqMessage.JMS_DESTINATION, throwingQueue);
        var message = ActiveMqMessages.newJsonActiveMqMessage(PAYLOAD_JSON, TARGET_MESSAGE_TYPE_1, properties);

        var consumer = FakeActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_QUEUE_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertThatExceptionOfType(UncheckedJMSException.class)
                .isThrownBy(() -> consumer.consume(message));
    }

    @Test
    void shouldThrowUncheckedJMSException_WhenTopicGetNameThrowsJMSException() {
        var throwingTopic = new ActiveMQTopic() {
            @Override
            public String getTopicName() throws JMSException {
                throw new JMSException("topic name failure");
            }
        };

        Map<String, Object> properties = new HashMap<>();
        properties.put(ActiveMqMessage.JMS_DESTINATION, throwingTopic);
        var message = ActiveMqMessages.newJsonActiveMqMessage(PAYLOAD_JSON, TARGET_MESSAGE_TYPE_1, properties);

        var consumer = FakeActiveMqConsumer.builder()
                .consumeMessagesOfType(TARGET_TOPIC_1_NAME, TARGET_MESSAGE_TYPE_1)
                .buildConsumer();

        assertThatExceptionOfType(UncheckedJMSException.class)
                .isThrownBy(() -> consumer.consume(message));
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
        return ActiveMqMessages.newJsonActiveMqMessage(body, messageType, createPropertyMapFrom(destination));
    }

    private static Map<String, Object> createPropertyMapFrom(ActiveMQDestination destination) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ActiveMqMessage.JMS_DESTINATION, destination);
        return properties;
    }

    private static void assertMessagesAreIgnored(FakeActiveMqConsumer consumer,
                                                 String destination,
                                                 ActiveMqMessage... messages) {

        Stream.of(messages).forEach(message -> assertMessageIsIgnored(consumer, destination, message));
    }

    private static void assertMessageIsIgnored(FakeActiveMqConsumer consumer,
                                               String destination,
                                               ActiveMqMessage message) {

        assertAll(
                () -> assertThat(consumer.consume(message)).isEqualTo(ActiveMqConsumer.Result.IGNORED),
                () -> assertThat(consumer.consumedHistory(destination)).isEmpty(),
                () -> assertThat(consumer.ignoredHistory(destination)).contains(message)
        );
    }

    private static void assertMessagesAreConsumed(FakeActiveMqConsumer consumer,
                                                  String destination,
                                                  ActiveMqMessage... messages) {

        Stream.of(messages).forEach(message -> assertMessageIsConsumed(consumer, destination, message));
    }

    private static void assertMessageIsConsumed(FakeActiveMqConsumer consumer,
                                                String destination,
                                                ActiveMqMessage message) {

        assertAll(
                () -> assertThat(consumer.consume(message)).isEqualTo(ActiveMqConsumer.Result.CONSUMED),
                () -> assertThat(consumer.consumedHistory(destination)).contains(message),
                () -> assertThat(consumer.ignoredHistory(destination)).isEmpty()
        );
    }
}

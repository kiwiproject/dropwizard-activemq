package org.kiwiproject.dropwizard.activemq.internal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.nonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.awaitility.Durations.ONE_SECOND;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.kiwiproject.base.KiwiStrings.f;
import static org.kiwiproject.collect.KiwiLists.first;
import static org.kiwiproject.collect.KiwiLists.isNotNullOrEmpty;
import static org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory.uniqueServiceName;
import static org.kiwiproject.test.assertj.KiwiAssertJ.assertPresentAndGet;
import static org.kiwiproject.test.assertj.dropwizard.metrics.HealthCheckResultAssertions.assertThatHealthCheck;
import static org.kiwiproject.test.constants.KiwiTestConstants.JSON_HELPER;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.Getter;
import org.awaitility.Durations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.kiwiproject.dropwizard.activemq.ActiveMqMessage;
import org.kiwiproject.dropwizard.activemq.test.junit.jupiter.EmbeddedActiveMqExtension;
import org.kiwiproject.dropwizard.activemq.test.mock.MockActiveMqConsumer;
import org.kiwiproject.dropwizard.activemq.test.util.ActiveMqTestUtils;
import org.kiwiproject.elucidation.client.ElucidationClient;
import org.kiwiproject.elucidation.client.ElucidationResult;
import org.kiwiproject.metrics.health.HealthCheckResults;
import org.kiwiproject.metrics.health.HealthStatus;
import org.kiwiproject.xml.KiwiXml;

import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

@DisplayName("Consumer")
class ConsumerTest {

    private static final String PAYLOAD = "hello out there!";
    private static final String SHORT_QUEUE_NAME = "tests";
    private static final String QUEUE_NAME = "dropwizardActiveMq.VirtualTopic." + SHORT_QUEUE_NAME;
    private static final String QUEUE = "queue:" + QUEUE_NAME;
    private static final String SPECIFIC_TEXT_MESSAGE_TYPE = "SPECIFIC_TEXT_MESSAGE_TYPE";
    private static final String GENERIC_TEXT_MESSAGE_TYPE = "TEXT_MESSAGE";
    private static final String BYTES_MESSAGE_TYPE = "BYTES_MESSAGE";

    @RegisterExtension
    final EmbeddedActiveMqExtension broker = new EmbeddedActiveMqExtension();

    private ConnectionFactory connectionFactory;
    private MessageProducer producer;
    private Consumer consumer;
    private Connection connection;
    private Session session;
    private ElucidationClient<String> elucidationClient;
    private String serviceName;

    @BeforeEach
    void setUp() throws JMSException {
        connectionFactory = broker.newConnectionFactory();
        connection = connectionFactory.createConnection();
        connection.start();

        session = ActiveMqTestUtils.createNonTransactedSession(connection);
        producer = ActiveMqTestUtils.createQueueProducer(session, QUEUE_NAME);

        // mock the ElucidationClient, so we don't attempt to contact a (non-existent) elucidation server
        elucidationClient = mockElucidationClient();
        var result = ElucidationResult.fromSkipMessage("Recorder not enabled");
        when(elucidationClient.recordNewEvent(anyString())).thenReturn(CompletableFuture.completedFuture(result));

        serviceName = uniqueServiceName();
    }

    @SuppressWarnings("unchecked")
    private static ElucidationClient<String> mockElucidationClient() {
        return mock(ElucidationClient.class);
    }

    @AfterEach
    void tearDown() throws Exception {
        producer.close();
        session.close();
        connection.close();

        if (nonNull(consumer)) {
            consumer.stop();
        }
    }

    @Test
    void shouldConsumeTextMessage_withJson() throws JMSException {
        var jmsConsumer = createMockActiveMqConsumerOf(SPECIFIC_TEXT_MESSAGE_TYPE);

        var textMessage = session.createTextMessage(JSON_HELPER.toJson(new InternalMessage()));
        producer.send(textMessage);

        // Need a minor wait to make sure the Consumer.loop ingests the message
        waitForSingleMessageAndCondition(jmsConsumer, () -> isNotNullOrEmpty(jmsConsumer.consumedHistory()));

        assertMessageWasConsumed(jmsConsumer);
        validateActiveMqMessage(first(jmsConsumer.consumedHistory()), "JSON", SPECIFIC_TEXT_MESSAGE_TYPE);

        assertConsumerIsHealthy();

        verifyElucidationClientGeneratesMessageProperly(SPECIFIC_TEXT_MESSAGE_TYPE);
    }

    @Test
    void shouldNotConsume_TextMessage_whenShouldConsumeReturnsFalse() throws JMSException {
        var jmsConsumer = createMockActiveMqConsumerThatWillNotConsume();

        var textMessage = session.createTextMessage(JSON_HELPER.toJson(new InternalMessage()));
        producer.send(textMessage);

        await().atMost(FIVE_SECONDS).until(() -> jmsConsumer.getShouldConsumeCount() > 0);

        assertThat(jmsConsumer.consumedHistory()).isEmpty();
        assertThat(jmsConsumer.consumedHistory(QUEUE_NAME)).isEmpty();
        assertThat(jmsConsumer.ignoredHistory()).isEmpty();

        assertConsumerIsHealthy();

        verifyNoInteractions(elucidationClient);
    }

    @Test
    void shouldIgnoreTextMessage_withBareText() throws JMSException {
        var jmsConsumer = createMockActiveMqConsumer();

        var textMessage = session.createTextMessage("this is not a valid message");
        producer.send(textMessage);

        // Need a minor wait to make sure the Consumer.loop ingests the message
        waitForSingleMessageAndCondition(jmsConsumer, () -> isNotNullOrEmpty(jmsConsumer.ignoredHistory()));

        assertThat(jmsConsumer.consumedHistory()).isEmpty();
        assertThat(jmsConsumer.ignoredHistory()).hasSize(1);
        assertThat(jmsConsumer.ignoredHistory(QUEUE_NAME)).hasSize(1);
        assertThat(first(jmsConsumer.ignoredHistory()).getMessageType()).contains("TEXT_MESSAGE");

        assertConsumerIsHealthy();

        verifyNoInteractions(elucidationClient);
    }

    @Test
    void shouldIgnoreTextMessage_withBadJson_whenNotRequireMessageType() throws JMSException {
        var jmsConsumer = createMockActiveMqConsumerOf(SPECIFIC_TEXT_MESSAGE_TYPE);
        
         // JSON has missing trailing double quote
        var textMessage = session.createTextMessage("""
                {
                    "payload": "with-broken-message-not-requiring-messageType
                }
                """);

        producer.send(textMessage);

         // Need a minor wait to make sure the Consumer.loop ingests the message
        waitForSingleMessageAndCondition(jmsConsumer, () -> isNotNullOrEmpty(jmsConsumer.ignoredHistory()));

         assertThat(jmsConsumer.consumedHistory()).isEmpty();
        assertThat(jmsConsumer.ignoredHistory()).hasSize(1);
        assertThat(jmsConsumer.ignoredHistory(QUEUE_NAME)).hasSize(1);
        assertThat(first(jmsConsumer.ignoredHistory()).getMessageType()).contains("UNKNOWN");

        assertConsumerIsHealthy();

        verifyNoInteractions(elucidationClient);
    }

    @Test
    void shouldConsumeTextMessage_withBadJson_requiringMessageType_andThrowException() throws JMSException {
        var jmsConsumer = createMockActiveMqConsumer();

        // JSON has missing trailing double quote
        var textMessage = session.createTextMessage("""
                {
                    "payload": "with-broken-message-requiring-messageType
                }
                """);

        producer.send(textMessage);

        // Need a minor wait to make sure the Consumer.loop ingests the message
        waitForSingleMessageAndCondition(jmsConsumer, () -> consumer.getErrors().get() > 0);

        assertThat(consumer.getErrors()).hasValue(1);
        assertThat(jmsConsumer.consumedHistory()).isEmpty();
        assertThat(jmsConsumer.ignoredHistory()).isEmpty();

        assertConsumerIsHealthy();

        verifyNoInteractions(elucidationClient);
    }

    @Test
    void shouldIgnoreTextMessage_withJsonWithoutMessageType_whenNotRequireMessageType() throws JMSException {
        var jmsConsumer = createMockActiveMqConsumerOf(SPECIFIC_TEXT_MESSAGE_TYPE);

        var textMessage = session.createTextMessage("""
                {
                    "payload": "with-no-messageType"
                }
                """);

        producer.send(textMessage);

        // Need a minor wait to make sure the Consumer.loop ingests the message
        waitForSingleMessageAndCondition(jmsConsumer, () -> isNotNullOrEmpty(jmsConsumer.ignoredHistory()));

        assertThat(jmsConsumer.consumedHistory()).isEmpty();
        assertThat(jmsConsumer.ignoredHistory()).hasSize(1);
        assertThat(jmsConsumer.ignoredHistory(QUEUE_NAME)).hasSize(1);
        assertThat(first(jmsConsumer.ignoredHistory()).getMessageType()).contains("UNKNOWN");

        assertConsumerIsHealthy();

        verifyNoInteractions(elucidationClient);
    }

    @Test
    void shouldConsumeTextMessage_withJsonWithoutMessageType_whenRequireMessageType_andThrowException() throws JMSException {
        var jmsConsumer = createMockActiveMqConsumer();

        var textMessage = session.createTextMessage("""
                {
                    "payload": "with-missing-required-messageType"
                }
                """);

        producer.send(textMessage);

        // Need a minor wait to make sure the Consumer.loop ingests the message
        waitForSingleMessageAndCondition(jmsConsumer, () -> consumer.getErrors().get() > 0);

        assertThat(consumer.getErrors()).hasValue(1);
        assertThat(jmsConsumer.consumedHistory()).isEmpty();
        assertThat(jmsConsumer.ignoredHistory()).isEmpty();

        assertConsumerIsHealthy();

        verifyNoInteractions(elucidationClient);
    }

    @Test
    void shouldConsumeTextMessage_withXML() throws JMSException {
        var jmsConsumer = createMockActiveMqConsumerOf(GENERIC_TEXT_MESSAGE_TYPE);

        var textMessage = session.createTextMessage(KiwiXml.toXml(new InternalMessage()));
        producer.send(textMessage);

        // Need a minor wait to make sure the Consumer.loop ingests the message
        waitForSingleMessageAndCondition(jmsConsumer, () -> isNotNullOrEmpty(jmsConsumer.consumedHistory()));

        assertMessageWasConsumed(jmsConsumer);
        validateActiveMqMessage(first(jmsConsumer.consumedHistory()), "TEXT", GENERIC_TEXT_MESSAGE_TYPE);

        assertConsumerIsHealthy();

        verifyElucidationClientGeneratesMessageProperly(GENERIC_TEXT_MESSAGE_TYPE);
    }

    @Test
    void shouldConsumeBytesMessage() throws JMSException {
        var jmsConsumer = createMockActiveMqConsumerOf(BYTES_MESSAGE_TYPE);

        var bytesMessage = session.createBytesMessage();
        bytesMessage.writeBytes(PAYLOAD.getBytes(UTF_8));

        producer.send(bytesMessage);

        // Need a minor wait to make sure the Consumer.loop ingests the message
        waitForSingleMessageAndCondition(jmsConsumer, () -> isNotNullOrEmpty(jmsConsumer.consumedHistory()));

        assertMessageWasConsumed(jmsConsumer);
        validateActiveMqMessage(first(jmsConsumer.consumedHistory()), "BYTES", BYTES_MESSAGE_TYPE);

        assertConsumerIsHealthy();

        verifyElucidationClientGeneratesMessageProperly(BYTES_MESSAGE_TYPE);
    }

    @Test
    void shouldIgnoreTextMessage_usingConsumerThatIgnoresSpecificMessageType() throws JMSException {
        var jmsConsumer = MockActiveMqConsumer.builder()
                .ignoringMessagesOfType(QUEUE_NAME, SPECIFIC_TEXT_MESSAGE_TYPE)
                .buildConsumer();

        createAndStartConsumerWith(jmsConsumer);

        var textMessage = session.createTextMessage(JSON_HELPER.toJson(new InternalMessage()));
        producer.send(textMessage);

        // Need a minor wait to make sure the Consumer.loop ingests the message
        waitForSingleMessageAndCondition(jmsConsumer, () -> isNotNullOrEmpty(jmsConsumer.ignoredHistory()));

        assertThat(jmsConsumer.consumedHistory()).isEmpty();
        assertThat(jmsConsumer.ignoredHistory()).hasSize(1);
        assertThat(jmsConsumer.ignoredHistory(QUEUE_NAME)).hasSize(1);

        assertConsumerIsHealthy();

        verifyNoInteractions(elucidationClient);
    }

    @Test
    void shouldStopWhenRequested() throws Exception {
        var jmsConsumer = MockActiveMqConsumer.builder().buildConsumer();

        createAndStartConsumerWith(jmsConsumer);

        // Manually stop the Consumer
        consumer.stop();

        waitUntilNotConsuming();

        assertThatHealthCheck(consumer.getHealthCheck())
                .isUnhealthy()
                .hasDetail(HealthCheckResults.SEVERITY_DETAIL, HealthStatus.WARN.name())
                .hasMessageEndingWith(f("is NOT consuming messages from {}!", QUEUE))
                .hasNoError();
    }

    @Test
    void shouldHandleUncaughtExceptions() throws JMSException {
        var error = new NoClassDefFoundError("com.acme.widget.SomeWidget");
        var jmsConsumer = MockActiveMqConsumer.builder()
                .throwError(error)
                .buildConsumer();

        createAndStartConsumerWith(jmsConsumer);

        var textMessage = session.createTextMessage(JSON_HELPER.toJson(new InternalMessage()));

        // Sending this should cause the jmsConsumer to throw the configured error
        producer.send(textMessage);

        waitUntilNotConsuming();

        assertThatHealthCheck(consumer.getHealthCheck())
                .isUnhealthy()
                .hasDetail(HealthCheckResults.SEVERITY_DETAIL, HealthStatus.CRITICAL.name())
                .hasMessageEndingWith(f("is NOT consuming messages from {} due to uncaught exception!", QUEUE))
                .hasErrorInstanceOf(error.getClass());
    }

    private void waitUntilNotConsuming() {
        await().atMost(Durations.TWO_SECONDS).until(() -> !consumer.isConsuming());
    }

    private MockActiveMqConsumer createMockActiveMqConsumerOf(String messageType) {
        var jmsConsumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(QUEUE_NAME, messageType)
                .validateBodyIsPresentOrThrowException()
                .buildConsumer();

        createAndStartConsumerWith(jmsConsumer);

        return jmsConsumer;
    }

    private MockActiveMqConsumer createMockActiveMqConsumer() {
        var jmsConsumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(QUEUE_NAME, SPECIFIC_TEXT_MESSAGE_TYPE)
                .validateBodyIsPresentOrThrowException()
                .validateMessageTypeIsPresentOrThrowException()
                .buildConsumer();

        createAndStartConsumerWith(jmsConsumer);

        return jmsConsumer;
    }

    private MockActiveMqConsumer createMockActiveMqConsumerThatWillNotConsume() {
        var jmsConsumer = MockActiveMqConsumer.builder()
                .consumeMessagesOfType(QUEUE_NAME, SPECIFIC_TEXT_MESSAGE_TYPE)
                .withShouldConsume(acttiveMqMessage -> false)
                .buildConsumer();

        createAndStartConsumerWith(jmsConsumer);

        return jmsConsumer;
    }

    private void createAndStartConsumerWith(MockActiveMqConsumer jmsConsumer) {
        consumer = new Consumer(connectionFactory, QUEUE, jmsConsumer, elucidationClient, serviceName);
        consumer.start();
    }

    private void waitForSingleMessageAndCondition(MockActiveMqConsumer jmsConsumer, Callable<Boolean> additionalCondition) {
        await().atMost(FIVE_SECONDS)
                .alias("received count")
                .until(() -> jmsConsumer.getReceivedCount() >= 1);
        
        if (nonNull(additionalCondition)) {
            await().atMost(ONE_SECOND)
                    .alias("additional condition")
                    .until(additionalCondition);
        }        
    }

    private void assertMessageWasConsumed(MockActiveMqConsumer jmsConsumer) {
        assertAll(
                () -> assertThat(jmsConsumer.consumedHistory()).hasSize(1),
                () -> assertThat(jmsConsumer.consumedHistory(QUEUE_NAME)).hasSize(1),
                () -> assertThat(jmsConsumer.ignoredHistory()).isEmpty()
        );
    }

    private void validateActiveMqMessage(ActiveMqMessage message, String expectedContentType, String expectedMessageType) {
        Optional<String> bodyOptional = message.getBody();
        String body = assertPresentAndGet(bodyOptional);

        assertThat(message.getContentType()).contains(ActiveMqMessage.ContentType.valueOf(expectedContentType));

        Optional<String> messageTypeOptional = message.getMessageType();
        String messageType = assertPresentAndGet(messageTypeOptional);
        assertThat(messageType).isEqualTo(expectedMessageType);

        var payload = switch (messageType) {
            case SPECIFIC_TEXT_MESSAGE_TYPE -> JSON_HELPER.getPath(body, "payload", String.class);
            case GENERIC_TEXT_MESSAGE_TYPE -> KiwiXml.toObject(body, InternalMessage.class).getPayload();
            case BYTES_MESSAGE_TYPE -> new String(Base64.getDecoder().decode(body), UTF_8);
            default -> throw new IllegalStateException(f("Received an unexpected type: {}", messageType));
        };

        assertThat(payload)
                .withFailMessage("For type [%s], the payload [%s] did not match expected value [%s]",
                        messageType, payload, PAYLOAD)
                .isEqualTo(PAYLOAD);

        Optional<String> correlationIDOptional = message.getJMSCorrelationID();
        var correlationId = assertPresentAndGet(correlationIDOptional);

        Optional<byte[]> correlationIDAsBytesOptional = message.getJMSCorrelationIDAsBytes();
        var correlationIdAsBytes = assertPresentAndGet(correlationIDAsBytesOptional);
        var correlationIdBytesAsString = new String(correlationIdAsBytes, UTF_8);

        assertThat(correlationId)
                .withFailMessage("Somehow the correlation ID [%s] and the UTF-7 string representation of the bytes [%s] don't match",
                        correlationId, correlationIdBytesAsString)
                .isEqualTo(correlationIdBytesAsString);

        validateOtherMessageFields(message);
    }

    private void validateOtherMessageFields(ActiveMqMessage message) {
        // JMS Message header fields
        validateFieldHasValue("JMSDeliveryMode", message.getJMSDeliveryMode());
        validateFieldHasValue("JMSDestinationAsQueue", message.getJMSDestinationAsQueue());
        validateFieldHasValue("JMSExpiration", message.getJMSExpiration());
        validateFieldHasValue("JMSMessageID", message.getJMSMessageID());
        validateFieldHasValue("JMSPriority", message.getJMSPriority());
        validateFieldHasValue("JMSRedelivered", message.getJMSRedelivered());
        validateFieldHasValue("JMSTimestamp", message.getJMSTimestamp());

        validateFieldIsEmpty("JMSDestinationAsTopic", message.getJMSDestinationAsTopic());
        validateFieldIsEmpty("JMSReplyTo", message.getJMSReplyTo());
        validateFieldIsEmpty("JMSType", message.getJMSType());

        // ActiveMQ Message header fields
        validateFieldHasValue("JMSXDeliveryCount", message.getJMSXDeliveryCount());
        validateFieldHasValue("JMSXGroupSeq", message.getJMSXGroupSeq());

        validateFieldIsEmpty("JMSXGroupId", message.getJMSXGroupId());
        validateFieldIsEmpty("JMSXUserId", message.getJMSXUserId());
    }

    private void validateFieldHasValue(String field, Optional<?> optional) {
        assertThat(optional)
                .withFailMessage("Expected field [{}] to contain a value", field)
                .isPresent();
    }

    private void validateFieldIsEmpty(String field, Optional<?> optional) {
        assertThat(optional)
                .withFailMessage("Did not expect field [{}] to contain a value", field)
                .isEmpty();
    }

    private void assertConsumerIsHealthy() {
        assertThatHealthCheck(consumer.getHealthCheck()).isHealthy();
    }

    private void verifyElucidationClientGeneratesMessageProperly(String messageType) {
        verify(elucidationClient, only()).recordNewEvent(SHORT_QUEUE_NAME + "::" + messageType);
    }

    @XmlRootElement
    @Getter  // needed for the JSON helper
    public static class InternalMessage {
        final String payload = PAYLOAD;
        final String messageType = SPECIFIC_TEXT_MESSAGE_TYPE;
    }

}
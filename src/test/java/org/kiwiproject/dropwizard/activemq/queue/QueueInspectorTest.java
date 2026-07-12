package org.kiwiproject.dropwizard.activemq.queue;

import static java.util.Objects.nonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.kiwiproject.dropwizard.activemq.util.MessageTypeParser.UNKNOWN_MESSAGE_TYPE;
import static org.kiwiproject.test.constants.KiwiTestConstants.JSON_HELPER;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Iterators;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import lombok.SneakyThrows;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.jms.pool.PooledConnection;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.dropwizard.activemq.test.junit.jupiter.EmbeddedActiveMqExtension;
import org.kiwiproject.dropwizard.activemq.util.UncheckedJMSException;

import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

@DisplayName("QueueInspector")
class QueueInspectorTest {

    @RegisterExtension
    final EmbeddedActiveMqExtension broker = new EmbeddedActiveMqExtension();

    private PooledConnectionFactory pooledConnectionFactory;
    private String queueName;
    private QueueInspector queueInspector;

    @BeforeEach
    void setUp() {
        pooledConnectionFactory = broker.newPooledConnectionFactory();
        queueName = "TestQ-" + System.currentTimeMillis();
        queueInspector = new QueueInspector(pooledConnectionFactory, JSON_HELPER);
        queueInspector.start();
    }

    @AfterEach
    void tearDown() {
        queueInspector.stop();
    }

    @Test
    void shouldThrow_OnConstruction_WhenActiveMqConfigIsNull() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new QueueInspector((ActiveMqConfig) null, JSON_HELPER));
    }

    @Test
    void shouldThrow_OnConstruction_WhenConnectionFactoryIsNull() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new QueueInspector((ConnectionFactory) null, JSON_HELPER));
    }

    @Test
    void shouldThrow_OnConstruction_WhenJsonHelperIsNull() {
        var connectionFactory = mock(ConnectionFactory.class);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> new QueueInspector(connectionFactory, null));
    }

    @Test
    void shouldConnect_OnConstruction_UsingActiveMqConfig() {
        var brokerUri = ((ActiveMQConnectionFactory) broker.newConnectionFactory()).getBrokerURL();
        var activeMqConfig = new ActiveMqConfig();
        activeMqConfig.setBrokerUri(brokerUri);

        var inspector = new QueueInspector(activeMqConfig, JSON_HELPER);
        try {
            inspector.start();
            assertThat(inspector.queueExists(queueName)).isFalse();
        } finally {
            inspector.stop();
        }
    }

    @Test
    void shouldThrow_WhenQueueExistsIsCalled_BeforeStart() {
        var inspector = new QueueInspector(mock(ConnectionFactory.class), JSON_HELPER);

        assertThatIllegalStateException()
                .isThrownBy(() -> inspector.queueExists(queueName))
                .withMessageContaining("not started");
    }

    @Test
    void shouldThrow_WhenGetQueueInfoIsCalled_BeforeStart() {
        var inspector = new QueueInspector(mock(ConnectionFactory.class), JSON_HELPER);

        assertThatIllegalStateException()
                .isThrownBy(() -> inspector.getQueueInfo(queueName))
                .withMessageContaining("not started");
    }

    @Test
    void shouldThrow_WhenQueueExistsIsCalled_AfterStop() {
        queueInspector.stop();

        assertThatIllegalStateException()
                .isThrownBy(() -> queueInspector.queueExists(queueName))
                .withMessageContaining("already stopped");
    }

    @Test
    void shouldThrow_WhenGetQueueInfoIsCalled_AfterStop() {
        queueInspector.stop();

        assertThatIllegalStateException()
                .isThrownBy(() -> queueInspector.getQueueInfo(queueName))
                .withMessageContaining("already stopped");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "  ", "\t"})
    void shouldThrow_WhenQueueExistsIsCalled_WithBlankQueueName(String blankQueueName) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> queueInspector.queueExists(blankQueueName))
                .withMessageContaining("queueName must not be blank");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "  ", "\t"})
    void shouldThrow_WhenGetQueueInfoIsCalled_WithBlankQueueName(String blankQueueName) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> queueInspector.getQueueInfo(blankQueueName))
                .withMessageContaining("queueName must not be blank");
    }

    @Test
    void shouldThrow_OnStart_WhenCannotCreateConnection() throws JMSException {
        var badConnectionFactory = mock(ConnectionFactory.class);
        var jmsException = new JMSException("Unable to create a connection");
        when(badConnectionFactory.createConnection()).thenThrow(jmsException);

        var inspector = new QueueInspector(badConnectionFactory, JSON_HELPER);

        assertThatExceptionOfType(UncheckedJMSException.class)
                .isThrownBy(inspector::start)
                .withCause(jmsException);

        //noinspection resource
        verify(badConnectionFactory).createConnection();
    }

    @Test
    void shouldThrow_OnStart_WhenCannotStartConnection() throws JMSException {
        var connectionFactory = mock(ConnectionFactory.class);
        var badConnection = mock(Connection.class);
        when(connectionFactory.createConnection()).thenReturn(badConnection);
        var jmsException = new JMSException("Unable to start connection");
        doThrow(jmsException).when(badConnection).start();

        var inspector = new QueueInspector(connectionFactory, JSON_HELPER);

        assertThatExceptionOfType(UncheckedJMSException.class)
                .isThrownBy(inspector::start)
                .withCause(jmsException);

        //noinspection resource
        verify(connectionFactory).createConnection();
        verify(badConnection).start();
        verify(badConnection).close();
    }

    @Test
    void shouldClearConnection_WhenStartFails_SoSubsequentCallsReportNotStarted() throws JMSException {
        var connectionFactory = mock(ConnectionFactory.class);
        var badConnection = mock(Connection.class);
        when(connectionFactory.createConnection()).thenReturn(badConnection);
        doThrow(new JMSException("Unable to start connection")).when(badConnection).start();

        var inspector = new QueueInspector(connectionFactory, JSON_HELPER);

        assertThatExceptionOfType(UncheckedJMSException.class).isThrownBy(inspector::start);

        assertThatIllegalStateException()
                .isThrownBy(() -> inspector.queueExists(queueName))
                .withMessageContaining("not started");
    }

    @Test
    void shouldThrow_WhenStartIsCalled_WhileAlreadyStarted() {
        assertThatIllegalStateException()
                .isThrownBy(() -> queueInspector.start())
                .withMessageContaining("already started");
    }

    @Test
    void shouldThrow_WhenConnectionIsNeitherActiveMQConnectionNorPooledConnection() throws JMSException {
        var connectionFactory = mock(ConnectionFactory.class);
        var plainConnection = mock(Connection.class);
        when(connectionFactory.createConnection()).thenReturn(plainConnection);

        var inspector = new QueueInspector(connectionFactory, JSON_HELPER);
        inspector.start();

        assertThatIllegalStateException()
                .isThrownBy(() -> inspector.queueExists(queueName))
                .withMessageContaining("expected PooledConnection but was");
    }

    @Test
    void shouldThrow_WhenPooledConnectionWrapsSomethingOtherThanActiveMQConnection() throws JMSException {
        var connectionFactory = mock(ConnectionFactory.class);
        var pooledConnection = mock(PooledConnection.class);
        var wrappedConnection = mock(Connection.class);
        when(connectionFactory.createConnection()).thenReturn(pooledConnection);
        when(pooledConnection.getConnection()).thenReturn(wrappedConnection);

        var inspector = new QueueInspector(connectionFactory, JSON_HELPER);
        inspector.start();

        assertThatIllegalStateException()
                .isThrownBy(() -> inspector.queueExists(queueName))
                .withMessageContaining("expected PooledConnection.connection to be ActiveMQConnection but was");
    }

    @Test
    void shouldCheckQueueExists_UsingActiveMQConnection_WhenItDoesNotExist() {
        doWithPlainAmqConnectionFactory((inspector, connectionFactory) ->
                assertThat(inspector.queueExists(queueName)).isFalse());
    }

    @Test
    void shouldCheckQueueExists_UsingActiveMQConnection_WhenItExists() {
        doWithPlainAmqConnectionFactory((inspector, connectionFactory) -> {
            createQueue(connectionFactory);

            assertThat(inspector.queueExists(queueName)).isTrue();
        });
    }

    @SneakyThrows
    private void doWithPlainAmqConnectionFactory(BiConsumer<QueueInspector, ConnectionFactory> consumer) {
        var plainConnectionFactory = broker.newConnectionFactory();
        QueueInspector inspector = null;
        try {
            inspector = new QueueInspector(plainConnectionFactory, JSON_HELPER);
            inspector.start();

            consumer.accept(inspector, plainConnectionFactory);
        } finally {
            if (nonNull(inspector)) {
                inspector.stop();
            }
        }
    }

    // Create a consumer for the queue; this relies on AMQ's default behavior, which
    // will auto-create the destination.
    @SneakyThrows
    private void createQueue(ConnectionFactory connectionFactory) {
        try (var connection = connectionFactory.createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            var queue = session.createQueue(queueName);

            try (var consumer = session.createConsumer(queue)) {
                consumer.receiveNoWait();
            }
        }
    }

    @Test
    void shouldCheckQueueExists_UsingPooledConnection_WhenItDoesNotExist() {
        assertThat(queueInspector.queueExists(queueName)).isFalse();
    }

    @Test
    void shouldCheckQueueExists_UsingPooledConnection_WhenItExists() {
        sendTextMessage();
        consumeSingleMessage();
        awaitMessageCount(0);

        assertThat(queueInspector.queueExists(queueName)).isTrue();
    }

    @Test
    void shouldInspectTheQueue_WhenItDoesNotExist() {
        var queueInfoInfo = queueInspector.getQueueInfo(queueName);

        assertAll(
                () -> assertThat(queueInfoInfo.exists()).isFalse(),
                () -> assertThat(queueInfoInfo.textMessageCount()).isZero(),
                () -> assertThat(queueInfoInfo.bytesMessageCount()).isZero(),
                () -> assertThat(queueInfoInfo.otherMessageCount()).isZero(),
                () -> assertThat(queueInfoInfo.messageTypeCounts()).isEmpty()
        );
    }

    @Test
    void shouldInspectTheQueue_WhenItIsEmpty() {
        sendTextMessage();
        consumeSingleMessage();
        awaitMessageCount(0);

        var queueInfo = queueInspector.getQueueInfo(queueName);

        assertAll(
                () -> assertThat(queueInfo.exists()).isTrue(),
                () -> assertThat(queueInfo.textMessageCount()).isZero(),
                () -> assertThat(queueInfo.bytesMessageCount()).isZero(),
                () -> assertThat(queueInfo.otherMessageCount()).isZero(),
                () -> assertThat(queueInfo.messageTypeCounts()).isEmpty()
        );
    }

    @Test
    void shouldInspectTheQueue_WhenItContainsTextMessage_WithOurMessageFormat() {
        sendJsonMessageWithMessageType();
        awaitMessageCount(1);

        var queueInfo = queueInspector.getQueueInfo(queueName);

        assertAll(
                () -> assertThat(queueInfo.exists()).isTrue(),
                () -> assertThat(queueInfo.textMessageCount()).isOne(),
                () -> assertThat(queueInfo.bytesMessageCount()).isZero(),
                () -> assertThat(queueInfo.otherMessageCount()).isZero(),
                () -> assertThat(queueInfo.messageTypeCounts()).containsEntry("STATUS_CHANGE", 1)
        );
    }

    @Test
    void shouldInspectTheQueue_WhenItContainsUnknownTextMessage() {
        sendTextMessage();
        awaitMessageCount(1);

        var queueInfo = queueInspector.getQueueInfo(queueName);

        assertAll(
                () -> assertThat(queueInfo.exists()).isTrue(),
                () -> assertThat(queueInfo.textMessageCount()).isOne(),
                () -> assertThat(queueInfo.bytesMessageCount()).isZero(),
                () -> assertThat(queueInfo.otherMessageCount()).isZero(),
                () -> assertThat(queueInfo.messageTypeCounts()).containsEntry(UNKNOWN_MESSAGE_TYPE, 1)
        );
    }

    @Test
    void shouldInspectTheQueue_WhenItContainsUnknownBytesMessage() {
        sendBytesMessage();
        awaitMessageCount(1);

        var queueInfo = queueInspector.getQueueInfo(queueName);

        assertAll(
                () -> assertThat(queueInfo.exists()).isTrue(),
                () -> assertThat(queueInfo.textMessageCount()).isZero(),
                () -> assertThat(queueInfo.bytesMessageCount()).isOne(),
                () -> assertThat(queueInfo.otherMessageCount()).isZero(),
                () -> assertThat(queueInfo.messageTypeCounts()).isEmpty()
        );
    }

    @Test
    void shouldInspectTheQueue_WhenItContainsOtherTypeOfMessage() {
        sendMapMessage();
        awaitMessageCount(1);

        var queueInfo = queueInspector.getQueueInfo(queueName);

        assertAll(
                () -> assertThat(queueInfo.exists()).isTrue(),
                () -> assertThat(queueInfo.textMessageCount()).isZero(),
                () -> assertThat(queueInfo.bytesMessageCount()).isZero(),
                () -> assertThat(queueInfo.otherMessageCount()).isOne(),
                () -> assertThat(queueInfo.messageTypeCounts()).isEmpty()
        );
    }

    @Test
    void shouldInspectTheQueue_WhenItContainsMultipleMessages() {
        sendTextMessage();
        sendJsonMessageWithMessageType();
        sendJsonMessageWithMessageType();
        sendBytesMessage();
        sendMapMessage();
        awaitMessageCount(5);

        var queueInfo = queueInspector.getQueueInfo(queueName);

        assertAll(
                () -> assertThat(queueInfo.exists()).isTrue(),
                () -> assertThat(queueInfo.textMessageCount()).isEqualTo(3),
                () -> assertThat(queueInfo.bytesMessageCount()).isOne(),
                () -> assertThat(queueInfo.otherMessageCount()).isOne(),
                () -> assertThat(queueInfo.messageTypeCounts())
                        .containsEntry("STATUS_CHANGE", 2)
                        .containsEntry(UNKNOWN_MESSAGE_TYPE, 1)
        );
    }

    private void sendTextMessage() {
        sendMessage(session -> session.createTextMessage("some random message"));
    }

    private void sendJsonMessageWithMessageType() {
        sendMessage(session -> session.createTextMessage("""
                {
                    "messageType": "STATUS_CHANGE"
                }
                """));
    }

    private void sendBytesMessage() {
        sendMessage(session -> {
            var bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes("Remember to drink your Ovaltine".getBytes(StandardCharsets.UTF_8));
            return bytesMessage;
        });
    }

    private void sendMapMessage() {
        sendMessage(session -> {
            var mapMessage = session.createMapMessage();
            mapMessage.setString("foo", "bar");
            mapMessage.setInt("answer", 42);
            return mapMessage;
        });
    }

    @SneakyThrows
    private void sendMessage(ThrowingFunction<Session, Message> fn) {
        try (var connection = pooledConnectionFactory.createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            var queue = session.createQueue(queueName);

            try (var producer = session.createProducer(queue)) {
                var message = fn.apply(session);
                producer.send(message);
            }
        }
    }

    @FunctionalInterface
    private interface ThrowingFunction<T, R> {
        R apply(T t) throws JMSException;
    }

    private void awaitMessageCount(int count) {
        await().atMost(FIVE_SECONDS).until(() -> queueInspector.queueExists(queueName) && countMessages() == count);
    }

    @SneakyThrows
    private int countMessages() {
        try (var connection = pooledConnectionFactory.createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            var queue = session.createQueue(queueName);

            try (var browser = session.createBrowser(queue)) {
                var iterator = browser.getEnumeration().asIterator();
                return Iterators.size(iterator);
            }
        }
    }

    @SneakyThrows
    private void consumeSingleMessage() {
        try (var connection = pooledConnectionFactory.createConnection();
             var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {

            var queue = session.createQueue(queueName);

            try (var consumer = session.createConsumer(queue)) {
                consumer.receive(500);
            }
        }
    }
}

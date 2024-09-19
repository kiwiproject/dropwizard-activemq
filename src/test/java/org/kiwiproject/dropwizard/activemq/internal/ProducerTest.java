package org.kiwiproject.dropwizard.activemq.internal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.kiwiproject.base.UUIDs.randomUUIDString;
import static org.kiwiproject.dropwizard.activemq.ActiveMqMessage.JMS_X_GROUP_ID;
import static org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory.uniqueServiceName;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.kiwiproject.dropwizard.activemq.ActiveMqMessage;
import org.mockito.ArgumentCaptor;
import org.mockito.verification.VerificationMode;

import java.time.Duration;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

@DisplayName("Producer")
class ProducerTest {

    private static final String QUEUE_NAME = "test";
    private static final String QUEUE = "queue:" + QUEUE_NAME;
    private static final String PAYLOAD = """
            {
                "payload": "test payload"
            }
            """;
    private static final Duration TIME_TO_LIVE = Duration.ofMinutes(1);
    private static final VerificationMode TWICE = times(2);

    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private MessageProducer jmsProducer;
    private String serviceName;

    @BeforeEach
    void setUp() throws JMSException {
        connectionFactory = mock(ConnectionFactory.class);
        connection = mock(Connection.class);
        session = mock(Session.class);
        jmsProducer = mock(MessageProducer.class);
        serviceName = uniqueServiceName();

        var destination = mock(Queue.class);

        when(connectionFactory.createConnection()).thenReturn(connection);
        when(connection.createSession(false, Session.AUTO_ACKNOWLEDGE)).thenReturn(session);
        when(session.createQueue(QUEUE_NAME)).thenReturn(destination);
        when(session.createProducer(destination)).thenReturn(jmsProducer);
        doNothing().when(jmsProducer).setTimeToLive(anyLong());

        var provider = spy(new Producer.ProducerProvider(connectionFactory, QUEUE, TIME_TO_LIVE, serviceName));
        when(provider.getSession()).thenReturn(session);
        when(provider.getMessageProducer()).thenReturn(jmsProducer);
        Correlation.CORRELATION_ID.remove();
    }

    @AfterEach
    void tearDown() {
        Correlation.CORRELATION_ID.remove();
    }

    @Test
    void shouldProduceMessage() throws JMSException {
        var producer = newConfiguredProducer();

        Correlation.CORRELATION_ID.set("My-Group-Id");

        var message = mock(TextMessage.class);
        when(session.createTextMessage(anyString())).thenReturn(message);
        when(message.getText()).thenReturn(PAYLOAD);
        when(message.getJMSCorrelationID()).thenReturn(null);

        producer.produce(PAYLOAD);

        verify(session).createTextMessage(PAYLOAD);

        var propertyNameCaptor = ArgumentCaptor.forClass(String.class);
        var propertyValueCaptor = ArgumentCaptor.forClass(String.class);
        verify(message, TWICE).setObjectProperty(propertyNameCaptor.capture(), propertyValueCaptor.capture());
        assertThat(propertyNameCaptor.getAllValues()).containsOnly(
                ActiveMqMessage.JMS_X_GROUP_ID, ActiveMqMessage.KIWI_AMQ_CONTENT_TYPE_KEY);
        assertThat(propertyValueCaptor.getAllValues()).containsOnly("My-Group-Id", "JSON");

        var messageCaptor = ArgumentCaptor.forClass(TextMessage.class);
        verify(jmsProducer).send(messageCaptor.capture());
        assertThat(messageCaptor.getValue().getText()).isEqualTo(PAYLOAD);

        verify(session).createTextMessage(PAYLOAD);
        verify(message).getText();
        verify(message).getJMSCorrelationID();
        verify(message).setJMSCorrelationID("My-Group-Id");
        verify(message).setObjectProperty(ActiveMqMessage.KIWI_AMQ_CONTENT_TYPE_KEY, "JSON");

        verifyJmsResourcesWereCreatedAndClosed();
    }

    @Test
    void shouldProduceMessage_WithNoCorrelationId() throws JMSException {
        var producer = newConfiguredProducer();

        var message = mock(TextMessage.class);

        when(session.createTextMessage(anyString())).thenReturn(message);
        when(message.getText()).thenReturn(PAYLOAD);
        when(message.getJMSCorrelationID()).thenReturn(null);

        producer.produce(PAYLOAD);

        verify(session).createTextMessage(PAYLOAD);

        var propertyNameCaptor = ArgumentCaptor.forClass(String.class);
        var propertyValueCaptor = ArgumentCaptor.forClass(String.class);
        verify(message).setObjectProperty(propertyNameCaptor.capture(), propertyValueCaptor.capture());
        assertThat(propertyNameCaptor.getAllValues()).containsOnly(ActiveMqMessage.KIWI_AMQ_CONTENT_TYPE_KEY);
        assertThat(propertyValueCaptor.getAllValues()).containsOnly("JSON");

        var messageCaptor = ArgumentCaptor.forClass(TextMessage.class);
        verify(jmsProducer).send(messageCaptor.capture());
        assertThat(messageCaptor.getValue().getText()).isEqualTo(PAYLOAD);

        verify(session).createTextMessage(PAYLOAD);
        verify(message).getText();
        verify(message).getJMSCorrelationID();
        verify(message, never()).setJMSCorrelationID(anyString());
        verify(message).setObjectProperty(ActiveMqMessage.KIWI_AMQ_CONTENT_TYPE_KEY, "JSON");

        verifyJmsResourcesWereCreatedAndClosed();
    }

    @Test
    void shouldProduceMessage_WithHeaders() throws JMSException {
        var producer = newConfiguredProducer();

        var message = mock(TextMessage.class);
        when(session.createTextMessage(anyString())).thenReturn(message);
        when(message.getText()).thenReturn(PAYLOAD);

        var headers = Map.<String, Object>of(
                "p1", "v1",
                "p2", "v2",
                "p3", "v3",
                "p4", "v4"
        );

        producer.produce(PAYLOAD, headers);

        var propertyNameCaptor = ArgumentCaptor.forClass(String.class);
        var propertyValueCaptor = ArgumentCaptor.forClass(String.class);
        verify(message, atLeast(4)).setObjectProperty(propertyNameCaptor.capture(), propertyValueCaptor.capture());
        assertThat(propertyNameCaptor.getAllValues()).containsOnly(
                ActiveMqMessage.KIWI_AMQ_CONTENT_TYPE_KEY, "p1", "p2", "p3", "p4");
        assertThat(propertyValueCaptor.getAllValues()).containsOnly(
                "JSON", "v1", "v2", "v3", "v4");
    }

    @Test
    void shouldSetMessageHeaders() throws JMSException {
        var producer = newConfiguredProducer();

        var message = mock(TextMessage.class);
        when(session.createTextMessage(anyString())).thenReturn(message);
        when(message.getText()).thenReturn(PAYLOAD);
        when(message.getJMSCorrelationID()).thenReturn(null);

        var headers = Map.<String, Object>of("firstProp", "firstValue", "secondProp", "secondValue");
        Producer.setHeaderProperties(message, headers);

        producer.produce(PAYLOAD);

        var propertyNameCaptor = ArgumentCaptor.forClass(String.class);
        var propertyValueCaptor = ArgumentCaptor.forClass(String.class);
        verify(message, times(3)).setObjectProperty(propertyNameCaptor.capture(), propertyValueCaptor.capture());

        assertThat(propertyNameCaptor.getAllValues()).contains("firstProp", "secondProp");
        assertThat(propertyValueCaptor.getAllValues()).contains("firstValue", "secondValue");
    }

    @Test
    void shouldProduceBytesMessage() throws JMSException {
        var producer = newConfiguredProducer();

        var message = mock(BytesMessage.class);
        when(session.createBytesMessage()).thenReturn(message);
        when(message.getJMSCorrelationID()).thenReturn(null);
        when(message.getStringProperty(anyString())).thenReturn(null);

        producer.produceBytesMessage(PAYLOAD.getBytes(UTF_8));

        var payloadCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(message).writeBytes(payloadCaptor.capture());
        assertThat(payloadCaptor.getValue()).isEqualTo(PAYLOAD.getBytes(UTF_8));

        verify(jmsProducer).send(message);

        verify(session).createBytesMessage();
        verify(message).getJMSCorrelationID();
        verify(message).getStringProperty(JMS_X_GROUP_ID);
        verify(message).setObjectProperty(ActiveMqMessage.KIWI_AMQ_CONTENT_TYPE_KEY, "BYTES");

        verifyJmsResourcesWereCreatedAndClosed();
        verifyNoMoreInteractions(message);
    }

    private Producer newConfiguredProducer() {
        var producer = new Producer(connectionFactory, QUEUE, false, serviceName);
        producer.setTimeToLive(TIME_TO_LIVE);

        return producer;
    }

    private void verifyJmsResourcesWereCreatedAndClosed() throws JMSException {
        // Due to spying the ProducerProvider constructor (see the setUp method), each call occurs
        // TWICE per test: once on the real object and once on the spy. That's why the following
        // verifications use times(2) !!!

        VerificationMode twice = TWICE;
        verify(connectionFactory, twice).createConnection();
        verify(connection, twice).start();
        verify(connection, twice).createSession(false, Session.AUTO_ACKNOWLEDGE);
        verify(session, twice).createProducer(any());
        verify(session, twice).createQueue(QUEUE_NAME);
        verify(jmsProducer, twice).setTimeToLive(TIME_TO_LIVE.toMillis());

        // However, there are only one set of close invocations per test.
        verify(jmsProducer).close();
        verify(session).close();
        verify(connection).close();

        verifyNoMoreInteractions(connectionFactory, connection, session, jmsProducer);
    }

    @Test
    void shouldSetJMSCorrelationId_whenMessageHasNoCorrelationId_AndCorrelationIdThreadLocalExists() throws JMSException {
        var id = randomUUIDString();
        Correlation.CORRELATION_ID.set(id);

        var message = mock(Message.class);
        when(message.getJMSCorrelationID()).thenReturn(null);

        Producer.setJMSCorrelationIdIfNecessary(message);

        verify(message).setJMSCorrelationID(id);
    }

    @Test
    void shouldNotSetJMSCorrelationId_whenMessageAlreadyHasCorrelationId_AndCorrelationIdThreadLocalExists() throws JMSException {
        var id = randomUUIDString();
        Correlation.CORRELATION_ID.set(id);

        var message = mock(Message.class);
        when(message.getJMSCorrelationID()).thenReturn(randomUUIDString());

        Producer.setJMSCorrelationIdIfNecessary(message);

        verify(message, never()).setJMSCorrelationID(anyString());
    }

    @Test
    void shouldNotSetJMSCorrelationId_whenMessageHasNoCorrelationId_AndCorrelationIdThreadLocalDoesNotExist() throws JMSException {
        Correlation.CORRELATION_ID.remove();

        var message = mock(Message.class);
        when(message.getJMSCorrelationID()).thenReturn(null);

        Producer.setJMSCorrelationIdIfNecessary(message);

        verify(message, never()).setJMSCorrelationID(anyString());
    }
}

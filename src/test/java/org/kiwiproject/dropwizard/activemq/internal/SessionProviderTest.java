package org.kiwiproject.dropwizard.activemq.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory.uniqueServiceName;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.kiwiproject.dropwizard.activemq.internal.DestinationIdentifier.ActorType;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.Session;

@DisplayName("SessionProvider")
class SessionProviderTest {

    private ConnectionFactory factory;
    private String serviceName;

    @BeforeEach
    void setUp() {
        factory = mock(ConnectionFactory.class);
        serviceName = uniqueServiceName();
    }

    @Test
    void shouldCreateNewInstance() throws JMSException {
        var connection = mock(Connection.class);
        var session = mock(Session.class);

        when(factory.createConnection()).thenReturn(connection);
        when(connection.createSession(anyBoolean(), anyInt())).thenReturn(session);

        try (var provider = new SessionProvider(factory, serviceName)) {
            verify(factory, only()).createConnection();
            verify(connection).createSession(false, Session.AUTO_ACKNOWLEDGE);

            assertThat(provider.getConnection()).isSameAs(connection);
            assertThat(provider.getSession()).isSameAs(session);
        }

        verify(session).close();
        verify(connection).close();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldThrowJMSException_CreatingInstance_WhenBadConnection() throws JMSException {
        when(factory.createConnection()).thenThrow(JMSException.class);

        assertThatExceptionOfType(JMSException.class)
                .isThrownBy(() -> new SessionProvider(factory, serviceName));

        verify(factory).createConnection();
    }

    @Test
    @SuppressWarnings("resource")
    void shouldThrowJMSException_CreatingInstance_WhenBadSession() throws JMSException {
        var connection = mock(Connection.class);
        when(factory.createConnection()).thenReturn(connection);
        when(connection.createSession(anyBoolean(), anyInt())).thenThrow(JMSException.class);

        assertThatExceptionOfType(JMSException.class)
                .isThrownBy(() -> new SessionProvider(factory, serviceName));

        verify(factory).createConnection();
        verify(connection).createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Nested
    class NewDestination {

        @ParameterizedTest
        @EnumSource(value = ActorType.class)
        void shouldCreateFixedTopicDestination(ActorType actorType) throws JMSException {
            var name = "fixedtopic:test";

            var session = checkSessionDestination(name, actorType);

            verify(session, only()).createTopic("test");
        }

        @Test
        void shouldCreateQueueDestination_ForVirtualTopicConsumer() throws JMSException {
            var name = "topic:test";

            var session = checkSessionDestination(name, ActorType.CONSUMER, "my-service");

            verify(session, only()).createQueue("Consumer.my-service.VirtualTopic.test");
        }

        @Test
        void shouldCreateTopicDestination_ForVirtualTopicProducer() throws JMSException {
            var name = "topic:test";

            var session = checkSessionDestination(name, ActorType.PRODUCER, "my-service");

            verify(session, only()).createTopic("VirtualTopic.test");
        }

        @ParameterizedTest
        @EnumSource(value = ActorType.class)
        void shouldCreateQueueDestination(ActorType actorType) throws JMSException {
            var name = "queue:test";

            var session = checkSessionDestination(name, actorType);

            verify(session, only()).createQueue("test");
        }

        @Test
        void shouldCreateDynamicDestinations() throws JMSException {
            var name = "*:queue://queueA,topic:topicB";

            var session = checkSessionDestination(name, ActorType.PRODUCER);

            verify(session, only()).createTopic("queue://queueA,topic:topicB");
        }

        @Test
        void shouldCreateQueueDestination_WhenGivenInvalidPrefix() throws JMSException {
            var name = "test";

            var session = checkSessionDestination(name, ActorType.PRODUCER);

            verify(session, only()).createQueue("test");
        }

        private Session checkSessionDestination(String destination, ActorType actorType) throws JMSException {
            return checkSessionDestination(destination, actorType, serviceName);
        }

        @SuppressWarnings("resource")
        private Session checkSessionDestination(String destination,
                                                ActorType actorType,
                                                String serviceName) throws JMSException {

            var connection = mock(Connection.class);
            var session = mock(Session.class);

            when(factory.createConnection()).thenReturn(connection);
            when(connection.createSession(false, Session.AUTO_ACKNOWLEDGE)).thenReturn(session);

            var provider = new SessionProvider(factory, serviceName);
            provider.newDestination(destination, session, actorType);

            return session;
        }
    }
}

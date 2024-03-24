package org.kiwiproject.dropwizard.activemq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.ONE_SECOND;
import static org.kiwiproject.dropwizard.activemq.test.util.ActiveMqTestUtils.createNonTransactedSession;
import static org.kiwiproject.dropwizard.activemq.test.util.ActiveMqTestUtils.createQueueConsumerMessageListener;
import static org.kiwiproject.dropwizard.activemq.test.util.ActiveMqTestUtils.createQueueProducer;
import static org.kiwiproject.dropwizard.activemq.test.util.ActiveMqTestUtils.createTopicConsumerMessageListener;
import static org.kiwiproject.dropwizard.activemq.test.util.ActiveMqTestUtils.createTopicProducer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.kiwiproject.dropwizard.activemq.test.junit.jupiter.EmbeddedActiveMqExtension;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

@DisplayName("DynamicProducers")
class DynamicProducersTest {

    @RegisterExtension
    final EmbeddedActiveMqExtension broker = new EmbeddedActiveMqExtension();

    private Connection connection;
    private Session session;

    @BeforeEach
    void setUp() throws JMSException {
        connection = broker.newPooledConnectionFactory().createConnection();
        connection.start();

        session = createNonTransactedSession(connection);
    }

    @AfterEach
    void tearDown() throws JMSException {
        session.close();
        connection.close();
    }

    @Test
    void shouldProduceToASingleQueue() throws JMSException {
        var listener = createQueueConsumerMessageListener(session, "queue-A");

        var messageProducer = createQueueProducer(session, "queue-A");
        messageProducer.send(session.createTextMessage("sample message"));

        await().atMost(ONE_SECOND).until(() -> listener.getCount() > 0);

        assertThat(listener.getCount()).isOne();
        assertThat(listener.getMessages()).containsExactly("sample message");
    }

    @Test
    void shouldProduceToMultipleQueueDestinations() throws JMSException {
        var listenerA = createQueueConsumerMessageListener(session, "queue-A");
        var listenerB = createQueueConsumerMessageListener(session, "queue-B");

        var messageProducer = createQueueProducer(session, "queue-A,queue-B");
        messageProducer.send(session.createTextMessage("sample message"));

        await().atMost(ONE_SECOND).until(() ->
                listenerA.getCount() > 0 && listenerB.getCount() > 0);

        assertThat(listenerA.getCount()).isOne();
        assertThat(listenerA.getMessages()).containsExactly("sample message");

        assertThat(listenerB.getCount()).isOne();
        assertThat(listenerB.getMessages()).containsExactly("sample message");
    }

    @Test
    void shouldProduceToASingleTopic() throws JMSException {
        var listener = createTopicConsumerMessageListener(session, "topic-A");

        var messageProducer = createTopicProducer(session, "topic-A");
        messageProducer.send(session.createTextMessage("sample message"));

        await().atMost(ONE_SECOND).until(() -> listener.getCount() > 0);

        assertThat(listener.getCount()).isOne();
        assertThat(listener.getMessages()).containsExactly("sample message");
    }

    @Test
    void shouldProduceToMultipleTopicDestinations() throws JMSException {
        var listenerA = createTopicConsumerMessageListener(session, "topic-A");
        var listenerB = createTopicConsumerMessageListener(session, "topic-B");

        var messageProducer = createTopicProducer(session, "topic-A,topic-B");
        messageProducer.send(session.createTextMessage("sample message"));

        await().atMost(ONE_SECOND).until(() ->
                listenerA.getCount() > 0 && listenerB.getCount() > 0);

        assertThat(listenerA.getCount()).isOne();
        assertThat(listenerA.getMessages()).containsExactly("sample message");

        assertThat(listenerB.getCount()).isOne();
        assertThat(listenerB.getMessages()).containsExactly("sample message");
    }
}

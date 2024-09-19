package org.kiwiproject.dropwizard.activemq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.kiwiproject.dropwizard.activemq.ActiveMqMessage.JMS_X_GROUP_ID;
import static org.kiwiproject.dropwizard.activemq.test.util.ActiveMqTestUtils.createConsumerMessageListener;
import static org.kiwiproject.dropwizard.activemq.test.util.ActiveMqTestUtils.createNonTransactedSession;
import static org.kiwiproject.dropwizard.activemq.test.util.ActiveMqTestUtils.createQueue;
import static org.kiwiproject.dropwizard.activemq.test.util.ActiveMqTestUtils.createTopic;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.kiwiproject.base.DefaultEnvironment;
import org.kiwiproject.dropwizard.activemq.test.junit.jupiter.EmbeddedActiveMqExtension;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

@DisplayName("RedundantVirtualTopicConsumers")
class RedundantVirtualTopicConsumersTest {

    private static final String VIRTUAL_TOPIC_NAME = "VirtualTopic.testVirtualTopic";

    @RegisterExtension
    final EmbeddedActiveMqExtension broker = new EmbeddedActiveMqExtension();

    private Connection connection;
    private Session session;

    @BeforeEach
    void setUp() throws JMSException {
        connection = broker.newConnectionFactory().createConnection();
        connection.start();

        session = createNonTransactedSession(connection);
    }

    @AfterEach
    void tearDown() throws JMSException {
        session.close();
        connection.close();
    }

    @Test
    void shouldAlternateConsumers() throws JMSException {
        var topic = createTopic(session, VIRTUAL_TOPIC_NAME);

        // create the redundant virtual consumers A1 and A2 in "Consumer.A" group
        var queueA = createQueue(session, "Consumer.A." + VIRTUAL_TOPIC_NAME);
        var listenerA1 = createConsumerMessageListener("A-1");
        var consumerA1 = session.createConsumer(queueA);
        consumerA1.setMessageListener(listenerA1);

        var listenerA2 = createConsumerMessageListener(session, "A-2", queueA);

        // single virtual consumer B1 in "Consumer.B" group
        var queueB = createQueue(session, "Consumer.B." + VIRTUAL_TOPIC_NAME);
        var listenerB1 = createConsumerMessageListener(session, "B-1", queueB);

        // non-virtual consumer C1 (consumes directory from the topic)
        var listenerC1 = createConsumerMessageListener(session, "C-1", topic);

        // Produce the messages
        var producer = session.createProducer(topic);
        var kiwiEnv = new DefaultEnvironment();
        var allMessages = new ArrayList<String>();
        var expectedA1Messages = new ArrayList<String>();
        var expectedA2Messages = new ArrayList<String>();

        for (int i = 1; i <= 20; i++) {
            var text = "Message " + i;
            allMessages.add(text);

            if (i == 11) {
                consumerA1.close();  // simulate client A1 disconnect after the 10th message
            }

            // collect A1 and A2 messages based on alternating and client A1 disconnect
            if (i > 10) {
                expectedA2Messages.add(text);
            } else if (isEven(i)) {
                expectedA2Messages.add(text);
            } else {
                expectedA1Messages.add(text);
            }

            var textMessage = session.createTextMessage(text);
            producer.send(textMessage);

            // Quick sleep so that we can guarantee topic message ordering (client C1)
            kiwiEnv.sleepQuietly(10, TimeUnit.MILLISECONDS);
        }

        // We should see messages alternating between consumers A1 and A2 up to the 10th message.
        // After that, all messages should go to A2 (b/c consumer A1 disconnected)
        // The individual virtual (B1) and non-virtual (C1) subscribers should each receive all messages

        assertAll(
                () -> assertThat(listenerA1.getCount()).isEqualTo(5),
                () -> assertThat(listenerA1.getMessages()).containsExactlyElementsOf(expectedA1Messages),

                () -> assertThat(listenerA2.getCount()).isEqualTo(15),
                () -> assertThat(listenerA2.getMessages()).containsExactlyElementsOf(expectedA2Messages),

                () -> assertThat(listenerB1.getCount()).isEqualTo(20),
                () -> assertThat(listenerB1.getMessages()).containsExactlyElementsOf(allMessages),

                () -> assertThat(listenerC1.getCount()).isEqualTo(20),
                () -> assertThat(listenerC1.getMessages()).containsExactlyElementsOf(allMessages)
        );
    }

    /**
     * When we specify a JMSXGroupID in a consumer group, we should see a single consumer
     * process all messages, until that consumer dies, after which another consumer takes over.
     * <p>
     * References:
     * <p>
     * <a href="https://activemq.apache.org/components/classic/documentation/message-groups">Message Groups</a>
     * <p>
     * <a href="https://activemq.apache.org/components/classic/documentation/exclusive-consumer">Exclusive Consumer</a>
     */
    @Test
    void shouldUseSameConsumer_InMessageGroup_UntilThatConsumerDies() throws JMSException {
        var topic = createTopic(session, VIRTUAL_TOPIC_NAME);

        // create the redundant virtual consumers A1 and A2 in "Consumer.A" group
        var queueA = createQueue(session, "Consumer.A." + VIRTUAL_TOPIC_NAME);
        var listenerA1 = createConsumerMessageListener("A-1");
        var consumerA1 = session.createConsumer(queueA);
        consumerA1.setMessageListener(listenerA1);

        var listenerA2 = createConsumerMessageListener(session, "A-2", queueA);

        // single virtual consumer B1 in "Consumer.B" group
        var queueB = createQueue(session, "Consumer.B." + VIRTUAL_TOPIC_NAME);
        var listenerB1 = createConsumerMessageListener(session, "B-1", queueB);

        // non-virtual consumer C1 (consumes directory from the topic)
        var listenerC1 = createConsumerMessageListener(session, "C-1", topic);

        // Produce the messages
        var producer = session.createProducer(topic);
        var kiwiEnv = new DefaultEnvironment();
        var allMessages = new ArrayList<String>();
        var expectedA1Messages = new ArrayList<String>();
        var expectedA2Messages = new ArrayList<String>();

        for (int i = 1; i <= 20; i++) {
            var text = "Message " + i;
            allMessages.add(text);

            if (i == 11) {
                consumerA1.close();  // simulate client A1 disconnect after the 10th message
            }

            // collect A1 and A2 messages based on message groups/exclusive consumer
            if (i > 10) {
                expectedA2Messages.add(text);
            } else {
                expectedA1Messages.add(text);
            }

            // create TextMessage with a group ID
            var textMessage = session.createTextMessage(text);
            textMessage.setStringProperty(JMS_X_GROUP_ID, "testGroup");

            producer.send(textMessage);

            // Quick sleep so that we can guarantee topic message ordering (client C1)
            kiwiEnv.sleepQuietly(10, TimeUnit.MILLISECONDS);
        }

        // We should see consumers A1 consuming all messages until it disconnects.
        // After that, all messages should go to A2.
        // The individual virtual (B1) and non-virtual (C1) subscribers should each receive all messages

        assertAll(
                () -> assertThat(listenerA1.getCount()).isEqualTo(10),
                () -> assertThat(listenerA1.getMessages()).containsExactlyElementsOf(expectedA1Messages),

                () -> assertThat(listenerA2.getCount()).isEqualTo(10),
                () -> assertThat(listenerA2.getMessages()).containsExactlyElementsOf(expectedA2Messages),

                () -> assertThat(listenerB1.getCount()).isEqualTo(20),
                () -> assertThat(listenerB1.getMessages()).containsExactlyElementsOf(allMessages),

                () -> assertThat(listenerC1.getCount()).isEqualTo(20),
                () -> assertThat(listenerC1.getMessages()).containsExactlyElementsOf(allMessages)
        );
    }

    private static boolean isEven(int n) {
        return (n & 1) == 0;
    }
}

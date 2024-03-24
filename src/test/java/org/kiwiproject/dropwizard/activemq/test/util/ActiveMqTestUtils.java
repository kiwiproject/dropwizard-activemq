package org.kiwiproject.dropwizard.activemq.test.util;

import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.base.KiwiStrings.f;

import lombok.experimental.UtilityClass;
import org.kiwiproject.dropwizard.activemq.util.UncheckedJMSException;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

@UtilityClass
public class ActiveMqTestUtils {

    private static final String LISTENER_PREFIX = "listener-";
    private static final boolean NOT_TRANSACTED = false;

    /**
     * Create a non-transacted Session with automatic acknowledgement for the given JMS connection.
     *
     * @param connection the JMS connection
     * @return a new Session
     * @see Connection#createSession(boolean, int)
     * @see Session#AUTO_ACKNOWLEDGE
     */
    public static Session createNonTransactedSession(Connection connection) {
        try {
            return connection.createSession(NOT_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            throw new UncheckedJMSException(e);
        }
    }

    /**
     * Create a new {@link ConsumerMessageListener} whose consumer name will be prefixed with "listener-".
     *
     * @param destinationName the ActiveMQ destination name (must not include a "queue://" or "topic://" prefix)
     * @return a new instance
     */
    public static ConsumerMessageListener createConsumerMessageListener(String destinationName) {
        return new ConsumerMessageListener(f("{}{}", LISTENER_PREFIX, destinationName));
    }

    /**
     * Create a new {@link ConsumerMessageListener} for a JMS {@link Queue} having the given name.
     *
     * @param session   the JMS session that the consumer listener will be attached to
     * @param queueName the ActiveMQ queue name (must not include the "queue://" prefix)
     * @return a new instance
     */
    public static ConsumerMessageListener createQueueConsumerMessageListener(Session session,
                                                                             String queueName) {
        return createConsumerMessageListener(session, createQueue(session, queueName));
    }

    /**
     * Create a new {@link ConsumerMessageListener} for a JMS {@link Topic} having the given name.
     *
     * @param session   the JMS session that the consumer listener will be attached to
     * @param topicName the ActiveMQ topic name (must not include the "topic://" prefix)
     * @return a new instance
     */
    public static ConsumerMessageListener createTopicConsumerMessageListener(Session session,
                                                                             String topicName) {
        return createConsumerMessageListener(session, createTopic(session, topicName));
    }

    /**
     * Create a new {@link ConsumerMessageListener} for the given session and destination.
     *
     * @param session     the JMS session that the consumer listener will be attached to
     * @param destination the JMS {@link Destination} for which to create the consumer and listener
     * @return a new instance
     */
    public static ConsumerMessageListener createConsumerMessageListener(Session session,
                                                                        Destination destination) {

        var destinationName = nameOf(destination);
        return createConsumerMessageListener(session, destinationName, destination);
    }

    /**
     * Create a new {@link ConsumerMessageListener} for the given session and destination.
     *
     * @param session         the JMS session that the consumer listener will be attached to
     * @param destinationName the name of the JMS destination (must not include a "queue://" or "topic://" prefix)
     * @param destination     the JMS {@link Destination} for which to create the consumer and listener
     * @return a new instance
     */
    public static ConsumerMessageListener createConsumerMessageListener(Session session,
                                                                        String destinationName,
                                                                        Destination destination) {

        var listener = createConsumerMessageListener(destinationName);

        try {
            session.createConsumer(destination).setMessageListener(listener);
            return listener;
        } catch (JMSException e) {
            throw new UncheckedJMSException(e);
        }
    }

    /**
     * Create a {@link MessageProducer} that can send messages to a {@link Queue} with the given name.
     *
     * @param session the JMS session
     * @param name    the name of the JMS Queue
     * @return a new MessageProducer instance
     * @throws UncheckedJMSException that wraps a thrown JMSException
     */
    public static MessageProducer createQueueProducer(Session session, String name) {
        try {
            return session.createProducer(createQueue(session, name));
        } catch (JMSException e) {
            throw new UncheckedJMSException(e);
        }
    }

    /**
     * Creates a {@link Queue}.
     *
     * @param session the JMS session
     * @param name    the name of the JMS Queue
     * @return a new Queue instance
     * @throws UncheckedJMSException that wraps a thrown JMSException
     */
    public static Queue createQueue(Session session, String name) {
        try {
            return session.createQueue(name);
        } catch (JMSException e) {
            throw new UncheckedJMSException(e);
        }
    }

    /**
     * Creates a {@link Topic}.
     *
     * @param session the JMS session
     * @param name    the name of the JMS Topic
     * @return a new Topic instance
     * @throws UncheckedJMSException that wraps a thrown JMSException
     */
    public static Topic createTopic(Session session, String name) {
        try {
            return session.createTopic(name);
        } catch (JMSException e) {
            throw new UncheckedJMSException(e);
        }
    }

    /**
     * Get the name of the destination, since JMS doesn't provide a way to get the name from
     * a {@link Destination} in a generic manner.
     *
     * @param destination the JMS Destination
     * @return the queue or topic name
     * @throws IllegalArgumentException if destination is blank, or is not a {@link Queue} or {@link Topic}
     * @throws UncheckedJMSException    wrapping any JMSException that are unexpectedly thrown
     */
    public static String nameOf(Destination destination) {
        checkArgumentNotNull(destination);

        try {
            if (destination instanceof Queue queue) {
                return queue.getQueueName();
            }

            if (destination instanceof Topic topic) {
                return topic.getTopicName();
            }

            var unsupportedType = destination.getClass().getName();
            throw new IllegalArgumentException("Unsupported Destination type: " + unsupportedType);

        } catch (JMSException e) {
            throw new UncheckedJMSException(e);
        }
    }
}

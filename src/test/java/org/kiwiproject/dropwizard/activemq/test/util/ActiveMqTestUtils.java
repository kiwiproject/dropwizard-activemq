package org.kiwiproject.dropwizard.activemq.test.util;

import static org.kiwiproject.base.KiwiStrings.f;

import lombok.experimental.UtilityClass;
import org.kiwiproject.dropwizard.activemq.util.UncheckedJMSException;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

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
     * Create a new {@link ConsumerMessageListener} whose consumer name will prefixed with "listener-".
     *
     * @param destinationName the ActiveMQ destination name
     * @return a new instance
     */
    public static ConsumerMessageListener createConsumerMessageListener(String destinationName) {
        return new ConsumerMessageListener(f("{}{}", LISTENER_PREFIX, destinationName));
    }

    /**
     * Create a new {@link ConsumerMessageListener} for a JMS {@link Queue} having the given name.
     *
     * @param session the JMS session that the consumer listener will be attached to
     * @param queueName the ActiveMQ queue name
     * @return a new instance
     */
    public static ConsumerMessageListener createQueueConsumerMessageListener(Session session, String queueName) {
        return createConsumerMessageListener(session, queueName, createQueue(session, queueName));
    }

    /**
     * Create a new {@link ConsumerMessageListener} for the given session and destination.
     *
     * @param session the JMS session that the consumer listener will be attached to
     * @param destinationName the name of the JMS destination
     * @param destination the JMS {@link Destination} for which to create the consumer and listener
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
     * @param name the name of the JMS Queue
     * @return a new MessageProducer instance
     * @throws UncheckedJMSException
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
     * @param name the name of the JMS Queue
     * @return a new Queue instance
     * @throws UncheckedJMSException
     */
    public static Queue createQueue(Session session, String name) {
        try {
            return session.createQueue(name);
        } catch (JMSException e) {
            throw new UncheckedJMSException(e);
        }
    }
}

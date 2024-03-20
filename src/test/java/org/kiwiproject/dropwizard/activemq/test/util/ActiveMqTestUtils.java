package org.kiwiproject.dropwizard.activemq.test.util;

import lombok.experimental.UtilityClass;

import org.kiwiproject.dropwizard.activemq.internal.UncheckedJMSException;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

@UtilityClass
public class ActiveMqTestUtils {

    private static final String LISTENER_PREFIX = "listener-";
    private static final boolean NOT_TRANSACTED = false;

    public static Session createNonTransactedSession(Connection connection) {
        try {
            return connection.createSession(NOT_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            throw new UncheckedJMSException(e);
        }
    }

    public static MessageProducer createProducerQueue(Session session, String name) {
        try {
            return session.createProducer(createQueue(session, name));
        } catch (JMSException e) {
            throw new UncheckedJMSException(e);
        }
    }

    private static Queue createQueue(Session session, String name) {
        try {
            return session.createQueue(name);
        } catch (JMSException e) {
            throw new UncheckedJMSException(e);
        }
    }
}

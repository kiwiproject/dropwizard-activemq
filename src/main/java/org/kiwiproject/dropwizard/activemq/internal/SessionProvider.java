package org.kiwiproject.dropwizard.activemq.internal;

import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.kiwiproject.dropwizard.activemq.util.Utils;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import java.util.Optional;

@Getter
@Slf4j
class SessionProvider implements AutoCloseable {

    static final String CLOSE_METHOD_NAME = "close";

    protected String serviceName;
    protected Connection connection;
    protected Session session;

    public SessionProvider(ConnectionFactory factory, String serviceName) throws JMSException {
        checkArgumentNotNull(factory);
        this.serviceName = requireNotBlank(serviceName);

        try {
            connection = factory.createConnection();
            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            Utils.safelyClose(session, connection);
            throw e;
        }
    }

    @Override
    public void close() {
        Utils.silentlyRun(session::close, connection::close);
    }

    Destination newDestination(String name, Session session, boolean isProducer) throws JMSException {
        Optional<DestinationIdentifier.DestinationInfo> info =
                DestinationIdentifier.evaluateDestinationName(name, isProducer, serviceName);

        // Using conditional here because of the checked JMSException, which can't be
        // thrown inside a lambda. This avoids wrapping and unwrapping checked exceptions.
        if (info.isPresent()) {
            var destinationInfo = info.get();
            return createDestination(session, destinationInfo);
        }

        LOG.error("Unexpected JMS configuration. Returning a Queue (which probably is not correct)." +
                " A {} destination should start with '{}', '{}', '{}', or '{}' but was: '{}'",
                isProducer ? "producer" : "consumer",
                DestinationIdentifier.FIXED_TOPIC_PREFIX,
                DestinationIdentifier.TOPIC_PREFIX,
                DestinationIdentifier.QUEUE_PREFIX,
                DestinationIdentifier.DYNAMIC_PREFIX,
                name);
        return session.createQueue(name);
    }

    private static Destination createDestination(Session session,
                                                 DestinationIdentifier.DestinationInfo destinationInfo)
            throws JMSException {

        var name = destinationInfo.getName();

        return switch (destinationInfo.getType()) {
            case QUEUE -> session.createQueue(name);
            case TOPIC -> session.createTopic(destinationInfo.getName());
        };
    }
}

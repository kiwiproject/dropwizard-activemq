package org.kiwiproject.dropwizard.activemq.queue;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.dropwizard.activemq.util.MessageTypeParser.UNKNOWN_MESSAGE_TYPE;
import static org.kiwiproject.io.KiwiIO.closeQuietly;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.jms.pool.PooledConnection;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.kiwiproject.dropwizard.activemq.ActiveMqHelper;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.dropwizard.activemq.util.MessageTypeParser;
import org.kiwiproject.dropwizard.activemq.util.UncheckedJMSException;
import org.kiwiproject.json.JsonHelper;

import java.util.Enumeration;
import java.util.LinkedHashMap;

/**
 * Class to inspect an ActiveMQ queue.
 */
public class QueueInspector implements Managed {

    private final ConnectionFactory connectionFactory;
    private final MessageTypeParser parser;

    private Connection connection;

    public QueueInspector(ActiveMqConfig activeMqConfig, JsonHelper jsonHelper) {
        this(newPooledConnectionFactory(activeMqConfig), jsonHelper);
    }

    private static PooledConnectionFactory newPooledConnectionFactory(ActiveMqConfig activeMqConfig) {
        checkArgumentNotNull(activeMqConfig, "activeMqConfig must not be null");
        return new ActiveMqHelper().newPooledConnectionFactory(activeMqConfig);
    }

    @VisibleForTesting
    QueueInspector(ConnectionFactory connectionFactory, JsonHelper jsonHelper) {
        this.parser = new MessageTypeParser(requireNotNull(jsonHelper, "jsonHelper must not be null"));
        this.connectionFactory = requireNotNull(connectionFactory, "connectionFactory must not be null");
    }

    /**
     * Starts the ActiveMQ connection.
     */
    @Override
    public void start() {
        checkState(isNull(connection), "already started - call stop() first");

        try {
            connection = connectionFactory.createConnection();
            connection.start();
        } catch (JMSException e) {
            closeQuietly(connection);
            connection = null;
            throw new UncheckedJMSException(e);
        }
    }

    /**
     * Closes the ActiveMQ connection.
     */
    @Override
    public void stop() {
        closeQuietly(connection);
        connection = null;
    }

    /**
     * Query ActiveMQ for information about the queue.
     *
     * @return a new {@link QueueInfo} instance
     */
    public QueueInfo getQueueInfo(String queueName) {
        try {
            return tryGetQueueInfo(queueName);
        } catch (JMSException e) {
            throw new UncheckedJMSException(e);
        }
    }

    private QueueInfo tryGetQueueInfo(String queueName) throws JMSException {
        if (!tryGetQueueExists(queueName)) {
            return QueueInfo.ofDoesNotExist();
        }

        try (var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            var queue = session.createQueue(queueName);

            try (var queueBrowser = session.createBrowser(queue)) {
                var messages = queueBrowser.getEnumeration();
                return extractQueueInfo(messages);
            }
        }
    }

    /**
     * Check if the queue exists.
     *
     * @return true if the queue exists, otherwise false
     */
    public boolean queueExists(String queueName) {
        try {
            return tryGetQueueExists(queueName);
        } catch (JMSException e) {
            throw new UncheckedJMSException(e);
        }
    }

    private boolean tryGetQueueExists(String queueName) throws JMSException {
        requireNotBlank(queueName, "queueName must not be blank");
        checkState(nonNull(connection), "not started or already stopped - call start() first");

        var activeMQConnection = getActiveMQConnection(connection);
        var destinationSource = activeMQConnection.getDestinationSource();
        return destinationSource.getQueues().stream()
                .anyMatch(activeMQQueue -> queueName.equals(activeMQQueue.getPhysicalName()));
    }

    private static ActiveMQConnection getActiveMQConnection(Connection connection) throws JMSException {
        if (connection instanceof ActiveMQConnection amqConnection) {
            return amqConnection;
        }

        checkState(connection instanceof PooledConnection,
                "expected PooledConnection but was %s", connection.getClass().getName());

        var pooledConnection = (PooledConnection) connection;
        var pooledConnectionConnection = pooledConnection.getConnection();

        checkState(pooledConnectionConnection instanceof ActiveMQConnection,
                "expected PooledConnection.connection to be ActiveMQConnection but was: %s",
                pooledConnectionConnection.getClass().getName());

        return (ActiveMQConnection) pooledConnectionConnection;
    }

    private QueueInfo extractQueueInfo(Enumeration<?> messages) throws JMSException {
        var textMessageCount = 0;
        var bytesMessageCount = 0;
        var otherMessageCount = 0;
        var messageTypeCounts = new LinkedHashMap<String, Integer>();

        while (messages.hasMoreElements()) {
            var message = (Message) messages.nextElement();

            if (message instanceof TextMessage textMessage) {
                ++textMessageCount;

                var maybeJson = textMessage.getText();
                var messageType = parser.findTypeSafe(maybeJson).orElse(UNKNOWN_MESSAGE_TYPE);
                messageTypeCounts.merge(messageType, 1, Integer::sum);

            } else if (message instanceof BytesMessage) {
                ++bytesMessageCount;
            } else {
                ++otherMessageCount;
            }
        }

        return QueueInfo.ofExists(textMessageCount, bytesMessageCount, otherMessageCount, messageTypeCounts);
    }

}

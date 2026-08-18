package org.kiwiproject.dropwizard.activemq.queue;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.dropwizard.activemq.util.MessageTypeParser.UNKNOWN_MESSAGE_TYPE;
import static org.kiwiproject.io.KiwiIO.closeQuietly;
import static org.kiwiproject.logging.LazyLogParameterSupplier.lazy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import io.dropwizard.lifecycle.Managed;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.jms.pool.PooledConnection;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.apache.commons.lang3.StringUtils;
import org.kiwiproject.dropwizard.activemq.ActiveMqHelper;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.dropwizard.activemq.util.MessageTypeParser;
import org.kiwiproject.dropwizard.activemq.util.UncheckedJMSException;
import org.kiwiproject.json.JsonHelper;

import java.util.Enumeration;
import java.util.LinkedHashMap;

/**
 * Class to inspect an ActiveMQ queue.
 * <p>
 * Queue discovery relies on ActiveMQ
 * <a href="https://activemq.apache.org/components/classic/documentation/advisory-message">destination advisories</a>,
 * which are enabled by default, and
 * <a href="https://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/advisory/DestinationSource.html">DestinationSource</a>.
 * If advisory support is disabled, or the connection is not permitted to consume
 * queue advisory messages, queue existence checks may not work correctly. Advisory
 * information is delivered asynchronously, so a queue may also be reported as
 * nonexistent briefly after startup.
 */
@Slf4j
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
     *
     * @throws IllegalStateException if start has already been called
     * @throws UncheckedJMSException if the connection could not be created or started
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
     * @throws IllegalArgumentException if queueName is blank
     * @throws IllegalStateException    if not started or already stopped
     * @throws UncheckedJMSException    if the query fails
     */
    public QueueInfo getQueueInfo(String queueName) {
        try {
            return tryGetQueueInfo(queueName);
        } catch (JMSException e) {
            throw new UncheckedJMSException(e);
        }
    }

    @VisibleForTesting
    QueueInfo tryGetQueueInfo(String queueName) throws JMSException {
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
     * @throws IllegalArgumentException if queueName is blank
     * @throws IllegalStateException    if not started or already stopped
     * @throws UncheckedJMSException    if the check fails
     */
    public boolean queueExists(String queueName) {
        try {
            return tryGetQueueExists(queueName);
        } catch (JMSException e) {
            throw new UncheckedJMSException(e);
        }
    }

    @VisibleForTesting
    boolean tryGetQueueExists(String queueName) throws JMSException {
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
                var messageType = findMessageTypeSafely(maybeJson);
                messageTypeCounts.merge(messageType, 1, Integer::sum);

            } else if (message instanceof BytesMessage) {
                ++bytesMessageCount;
            } else {
                ++otherMessageCount;
            }
        }

        return QueueInfo.ofExists(textMessageCount, bytesMessageCount, otherMessageCount, messageTypeCounts);
    }

    /**
     * Delegates to {@link MessageTypeParser#findTypeSafe(String)}, but also absorbs the
     * {@link VerifyException} it can throw for a message with conflicting type values (see its Javadoc).
     * A single such message must not abort inspection of the rest of the queue, so it is counted as
     * {@link MessageTypeParser#UNKNOWN_MESSAGE_TYPE} instead.
     */
    private String findMessageTypeSafely(String maybeJson) {
        try {
            return parser.findTypeSafe(maybeJson).orElse(UNKNOWN_MESSAGE_TYPE);
        } catch (VerifyException e) {
            LOG.warn("Message contains conflicting message type values, counting as {} (msg: '{}', enable TRACE for full message content)",
                    UNKNOWN_MESSAGE_TYPE, lazy(() -> StringUtils.abbreviate(maybeJson, 50)), e);
            LOG.trace("Message content: {}", maybeJson);
            return UNKNOWN_MESSAGE_TYPE;
        }
    }

}

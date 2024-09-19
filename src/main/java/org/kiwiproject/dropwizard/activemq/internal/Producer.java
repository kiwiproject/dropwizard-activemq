package org.kiwiproject.dropwizard.activemq.internal;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.base.KiwiStrings.f;
import static org.kiwiproject.dropwizard.activemq.ActiveMqMessage.JMS_X_GROUP_ID;
import static org.kiwiproject.dropwizard.activemq.ActiveMqMessage.KIWI_AMQ_CONTENT_TYPE_KEY;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.kiwiproject.dropwizard.activemq.ActiveMqMessage;
import org.kiwiproject.dropwizard.activemq.exception.ActiveMqMessageHeaderException;
import org.kiwiproject.dropwizard.activemq.util.Utils;
import org.kiwiproject.dropwizard.activemq.util.Utils.FunctionThrowsException;

import java.time.Duration;
import java.util.Map;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

/**
 * This internal class uses JMS {@link javax.jms.MessageProducer} instances to send messages.
 */
@Slf4j
public class Producer {

    private final ConnectionFactory factory;
    private final String destination;

    @Getter
    private final boolean isDefaultProducer;

    private final String serviceName;

    // TODO Why is this NOT part of the constructor?
    @Setter
    private Duration timeToLive;

    public Producer(ConnectionFactory factory,
                    String destination,
                    boolean isDefaultProducer,
                    String serviceName) {

        this.factory = requireNotNull(factory);
        this.destination = requireNotBlank(destination);
        this.isDefaultProducer = isDefaultProducer;
        this.serviceName = requireNotBlank(serviceName);
    }

    /**
     * Set properties on the message using {@link Message#setObjectProperty(String, Object)}.
     *
     * @param message    the JMS message
     * @param properties the properties to set
     */
    public static void setHeaderProperties(Message message, Map<String, Object> properties) {
        // ActiveMQ won't allow anything in its "ObjectProperty" fields
        // except "objectified" primitives, String, Map, and List objects.
        //
        // See org.apache.activemq.command.ActiveMQMessage#checkValidObject

        properties.forEach((key, value) -> setJmsProperty(message, key, value));
    }

    public void produce(String payload) {
        produce(payload, destination);
    }

    public void produce(String payload, String destination) {
        produce(payload, destination, Map.of());
    }

    public void produce(String payload, Map<String, Object> headers) {
        produce(payload, destination, headers);
    }

    public void produce(String payload, String destination, Map<String, Object> headers) {
        send(destination, session -> {
            var message = session.createTextMessage(payload);

            setHeaderProperties(message, headers);
            setJMSCorrelationIdIfNecessary(message);
            setGroupIdIfNecessary(message);
            setContentType(message, TypesDetector.determineContentTypeOf(payload));

            return message;
        });
    }

    public void produceBytesMessage(byte[] payload) {
        produceBytesMessage(payload, destination);
    }

    public void produceBytesMessage(byte[] payload, String destination) {
        send(destination, session -> {
            var message = session.createBytesMessage();
            message.writeBytes(payload);

            setJMSCorrelationIdIfNecessary(message);
            setGroupIdIfNecessary(message);
            setContentType(message, ActiveMqMessage.ContentType.BYTES);

            return message;
        });
    }

    @VisibleForTesting
    void setGroupIdIfNecessary(Message message) throws JMSException {
        var defaultGroupId = Correlation.CORRELATION_ID.get();

        if (isNull(message.getStringProperty(JMS_X_GROUP_ID)) && nonNull(defaultGroupId)) {
            setJmsProperty(message, JMS_X_GROUP_ID, defaultGroupId);
        }
    }

    @VisibleForTesting
    static void setJMSCorrelationIdIfNecessary(Message message) throws JMSException {
        var correlationId = Correlation.CORRELATION_ID.get();

        if (isNull(message.getJMSCorrelationID()) && nonNull(correlationId)) {
            LOG.trace("Setting JMSCorrelationID to {}", correlationId);
            message.setJMSCorrelationID(correlationId);
        }
    }

    private static void setContentType(Message message, ActiveMqMessage.ContentType contentType) {
        setJmsProperty(message, KIWI_AMQ_CONTENT_TYPE_KEY, contentType.name());
    }

    private static void setJmsProperty(Message message, String key, Object value) {
        try {
            LOG.trace("Setting ObjectProperty {} to {}", key, value);
            message.setObjectProperty(key, value);
        } catch (JMSException e) {
            LOG.debug("Failed to set property with key: [{}] and value [{}]", key, value);
            var errorMessage = f("Unable to set property with key: [{}]. Error code: {} Reason: {}",
                    key, e.getErrorCode(), e.getMessage());
            throw new ActiveMqMessageHeaderException(errorMessage, e);
        }
    }

    private void send(String destination, FunctionThrowsException<Session, Message> messageBuilder) {
        try (var provider = new ProducerProvider(factory, destination, timeToLive, serviceName)) {
            var message = messageBuilder.apply(provider.getSession());

            LOG.trace("Sending {} message to {}", message.getClass().getSimpleName(), destination);
            provider.getMessageProducer().send(message);
        } catch (Exception e) {
            LOG.error("Error sending message via JMS to {}", destination, e);
            // TODO Should this throw an exception? It never did (and it's been like this since 2016...)
        }
    }

    @Getter
    static class ProducerProvider extends SessionProvider {

        private MessageProducer messageProducer;

        ProducerProvider(ConnectionFactory factory,
                         String destination,
                         Duration timeToLive,
                         String serviceName) throws JMSException {

            super(factory, serviceName);

            try {
                messageProducer = session.createProducer(newDestination(destination, session, true));

                if (nonNull(timeToLive)) {
                    var timeToLiveMillis = timeToLive.toMillis();
                    LOG.trace("Setting timeToLive on producer to {} to {}ms", destination, timeToLiveMillis);
                    messageProducer.setTimeToLive(timeToLiveMillis);
                }
            } catch (JMSException e) {
                Utils.safelyClose(messageProducer, session, connection);
                throw e;
            }
        }

        @Override
        public void close() {
            Utils.silentlyRun(messageProducer::close);
            super.close();
        }
    }
}

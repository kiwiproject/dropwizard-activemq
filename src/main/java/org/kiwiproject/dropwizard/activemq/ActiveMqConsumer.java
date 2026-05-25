package org.kiwiproject.dropwizard.activemq;

import static org.kiwiproject.dropwizard.activemq.util.MessageTypeParser.UNKNOWN_MESSAGE_TYPE;

import org.kiwiproject.dropwizard.activemq.exception.ActiveMqMessageException;
import org.kiwiproject.dropwizard.activemq.exception.ActiveMqMessageInvalidMessageTypeException;
import org.kiwiproject.dropwizard.activemq.exception.ActiveMqMessageMissingBodyException;
import org.kiwiproject.dropwizard.activemq.util.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface to implement for consuming JMS messages delivered by the dropwizard-activemq library.
 * <p>
 * Implementations are registered with
 * {@link org.kiwiproject.dropwizard.activemq.DropwizardActiveMq#startConsumer(ActiveMqConsumer, String[])}
 * and will have {@link #consume(ActiveMqMessage)} called for each incoming message.
 */
public interface ActiveMqConsumer {

    /**
     * The result of consuming a JMS message.
     */
    enum Result {
        /** The message was consumed successfully. */
        CONSUMED,

        /** The message was intentionally ignored. */
        IGNORED
    }

    /**
     * Consume a JMS message.
     *
     * @param message the {@link ActiveMqMessage} to consume
     * @return {@link Result#CONSUMED} if the message was consumed, {@link Result#IGNORED} otherwise
     */
    Result consume(ActiveMqMessage message);

    /**
     * This method can be overridden to provide custom logic that determines whether an incoming
     * message should be consumed. If this method returns false, the message is effectively ignored
     * because {@link #consume(ActiveMqMessage)} won't be called.
     * <p>
     * The default is {@code true}, meaning all messages will be consumed.
     *
     * @param message the incoming {@link ActiveMqMessage}
     * @return {@code true} if the message should be consumed, otherwise {@code false}
     */
    default boolean shouldConsume(ActiveMqMessage message) {
        return true;
    }

    /**
     * Validate that the {@link ActiveMqMessage} has a non-empty body
     * or throw a {@link ActiveMqMessageMissingBodyException}.
     *
     * @param message the message to validate
     * @return the body
     * @throws ActiveMqMessageMissingBodyException if the body is {@code null} or blank
     */
    default String requireValidBody(ActiveMqMessage message) {
        return message.getBody().orElseThrow(ActiveMqMessageMissingBodyException::new);
    }

    /**
     * Validate that the {@link ActiveMqMessage} has a valid message type which is not
     * equal to {@link MessageTypeParser#UNKNOWN_MESSAGE_TYPE}.
     * <p>
     * Only use this when a message is required to have a <strong>known</strong> message type.
     *
     * @param message the message to validate
     * @return the message type
     * @throws ActiveMqMessageInvalidMessageTypeException if the message type is missing, blank, or
     *                                                    equals the UNKNOWN_MESSAGE_TYPE
     */
    default String requireValidMessageType(ActiveMqMessage message) {
        var messageType = message.getMessageType()
                .orElseThrow(() -> new ActiveMqMessageInvalidMessageTypeException("No message type present"));

        if (UNKNOWN_MESSAGE_TYPE.equals(messageType)) {
            throw new ActiveMqMessageInvalidMessageTypeException("Type parser was unable to identify the message type");
        }

        return messageType;
    }

    /**
     * Handle any {@link ActiveMqMessageException} that may be generated be the ActiveMqConsumer.
     * <p>
     * This method is primarily exposed for testing purposes, but can also be used by implementations
     * of {@link ActiveMqConsumer} for custom exception handling.
     * <p>
     * This default implementation logs a WARN-level message and also a TRACE-level message containing
     * the actual message and the stack trace. If many messages are failing, this allows you to change
     * the log level to TRACE (temporarily) to view message contents for aid when debugging a problem.
     *
     * @param message the message
     * @param e       the exception to handle
     */
    default void handleException(ActiveMqMessage message, ActiveMqMessageException e) {
        var logger = getLogger();

        logger.warn("Failed to process an ActiveMqMessage: " +
                        "exception type={}, contentType={}, messageType={}, reason={} (body and stack trace at TRACE level)",
                e.getCategory(),
                message.getContentType(),
                message.getMessageType(),
                e.getMessage()
        );

        logger.trace("Message: {}", message.getBody(), e);
    }

    private static Logger getLogger() {
        return LoggerFactory.getLogger(ActiveMqConsumer.class);
    }
}

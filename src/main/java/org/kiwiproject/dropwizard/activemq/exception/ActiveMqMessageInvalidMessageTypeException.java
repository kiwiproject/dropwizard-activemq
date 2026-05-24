package org.kiwiproject.dropwizard.activemq.exception;

/**
 * Thrown when a JMS message has a missing, blank, or unrecognized message type.
 */
public class ActiveMqMessageInvalidMessageTypeException extends ActiveMqMessageException {

    private static final String CATEGORY = "[Missing or Unknown Message Type]";

    public ActiveMqMessageInvalidMessageTypeException(String message) {
        super(CATEGORY, message);
    }
}

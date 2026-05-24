package org.kiwiproject.dropwizard.activemq.exception;

/**
 * Thrown when setting a JMS message property (header) fails.
 */
public class ActiveMqMessageHeaderException extends ActiveMqMessageException {

    private static final String CATEGORY = "[Error setting Message Property]";

    public ActiveMqMessageHeaderException(String message, Throwable throwable) {
        super(CATEGORY, message, throwable);
    }
}

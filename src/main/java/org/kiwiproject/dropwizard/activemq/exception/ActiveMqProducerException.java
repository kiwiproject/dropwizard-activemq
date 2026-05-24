package org.kiwiproject.dropwizard.activemq.exception;

/**
 * Thrown when an ActiveMQ producer fails to send a message.
 */
public class ActiveMqProducerException extends ActiveMqMessageException {

     private static final String CATEGORY = "[Producer Send Failure]";

    public ActiveMqProducerException(String message, Throwable throwable) {
        super(CATEGORY, message, throwable);
    }
}

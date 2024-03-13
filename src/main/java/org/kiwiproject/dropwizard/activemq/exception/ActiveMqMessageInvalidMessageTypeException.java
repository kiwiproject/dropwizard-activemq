package org.kiwiproject.dropwizard.activemq.exception;

public class ActiveMqMessageInvalidMessageTypeException extends ActiveMqMessageException {

    private static final String CATEGORY = "[Missing or Unknown Message Type]";

    public ActiveMqMessageInvalidMessageTypeException(String message) {
        super(CATEGORY, message);
    }
}

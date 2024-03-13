package org.kiwiproject.dropwizard.activemq.exception;

public class ActiveMqMessageMissingBodyException extends ActiveMqMessageException {

    private static final String CATEGORY = "[Missing Body]";

    public ActiveMqMessageMissingBodyException() {
        super(CATEGORY, "No body attached to ActiveMqMessage");
    }
}

package org.kiwiproject.dropwizard.activemq.exception;

import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;

import lombok.Getter;

/**
 * Base unchecked exception for errors that occur while processing an ActiveMQ JMS message.
 * Carries a {@code category} string that subclasses use to identify the type of failure.
 */
public class ActiveMqMessageException extends RuntimeException {

    @Getter
    private final String category;

    ActiveMqMessageException(String category, String message) {
        this(category, message, null);
    }

    public ActiveMqMessageException(String category, String message, Throwable throwable) {
        super(message, throwable);
        this.category = requireNotBlank(category);
    }
}

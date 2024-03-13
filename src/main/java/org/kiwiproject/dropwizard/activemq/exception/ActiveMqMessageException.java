package org.kiwiproject.dropwizard.activemq.exception;

import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;

import lombok.Getter;

public class ActiveMqMessageException extends RuntimeException {

    @Getter
    private final String category;

    ActiveMqMessageException(String category, String message) {
        this(category, message, null);
    }

    public ActiveMqMessageException(String category, String message, Throwable throwable) {
        super(message, throwable);
        this.category =  requireNotBlank(category);
    }
}

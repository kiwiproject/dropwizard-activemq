package org.kiwiproject.dropwizard.activemq.util;

/**
 * Exception thrown when there is an error finding the message type in in a JSON message.
 */
public class MessageTypeParsingException extends RuntimeException {

    public MessageTypeParsingException(String msg) {
        super(msg);
    }

    public MessageTypeParsingException(Throwable ex) {
        super(ex);
    }
}

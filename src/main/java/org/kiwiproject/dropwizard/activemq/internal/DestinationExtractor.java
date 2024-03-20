package org.kiwiproject.dropwizard.activemq.internal;

import lombok.experimental.UtilityClass;

import org.apache.activemq.command.ActiveMQDestination;
import org.kiwiproject.dropwizard.activemq.ActiveMqMessage;

import java.util.regex.Pattern;

@UtilityClass
class DestinationExtractor {

    // TODO

    private static final String DELIMITER = "::";

    static final Pattern DELIMITER_SPLITTER = Pattern.compile(DELIMITER);

    static final String BYTES_MESSAGE_TYPE = ActiveMqMessage.ContentType.BYTES.convertToMessageType();

    static String simplifyDestination(ActiveMQDestination originalDestination) {
        return simplifyDestination(originalDestination.toString());
    }

    private static String simplifyDestination(String string) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'simplifyDestination'");
    }

    static String createElucidationDestination(String destination, String messageType) {
        return String.join(DELIMITER, destination, messageType);
    }
}

package org.kiwiproject.dropwizard.activemq.internal;

import lombok.experimental.UtilityClass;

import org.apache.activemq.command.ActiveMQDestination;

@UtilityClass
class DestinationExtractor {

    // TODO

    private static final String DELIMITER = "::";

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

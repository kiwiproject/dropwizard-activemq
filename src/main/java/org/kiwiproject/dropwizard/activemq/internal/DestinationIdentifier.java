package org.kiwiproject.dropwizard.activemq.internal;

import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class DestinationIdentifier {

    // TODO

    public enum DestinationType {
        QUEUE, TOPIC
    }

    public static Optional<DestinationInfo> evaluateDestinationName(String name, boolean isProducer, String serviceName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'evaluateDestinationName'");
    }

    @Getter
    @Setter
    public static class DestinationInfo {
        private DestinationType type;
        private String name;

        DestinationInfo(DestinationType type, String name) {
            this.type = requireNotNull(type);
            this.name = requireNotBlank(name);
        }
    }
}

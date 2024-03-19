package org.kiwiproject.dropwizard.activemq.internal;

import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.base.KiwiStrings.f;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class DestinationIdentifier {

    /**
     * The "fixedtopic" prefix allows for non-virtual (regular JMS) topics.
     * Virtual topics are the default here.
     */
    static final String FIXED_TOPIC_PREFIX = "fixedtopic:";

    /**
     * Virtual topic prefix.
     */
    static final String TOPIC_PREFIX = "topic:";

    /**
     * Queue prefix.
     */
    static final String QUEUE_PREFIX = "queue:";

    /**
     * Dynamic destination prefix.
     */
    static final String DYNAMIC_PREFIX = "*:";

    public enum DestinationType {
        QUEUE, TOPIC
    }

    public static Optional<DestinationInfo> evaluateDestinationName(String name, boolean isProducer, String serviceName) {
        DestinationInfo dest;

        if (name.startsWith(FIXED_TOPIC_PREFIX)) {  // normal JMS topic

            var trimmedName = trimPrefix(name, FIXED_TOPIC_PREFIX);
            dest = new DestinationInfo(DestinationType.TOPIC, trimmedName);

        } else if (name.startsWith(TOPIC_PREFIX)) {  // this is an ActiveMQ virtual topic

            // Producers all produce to VirtualTopic.<name> while consumers all
            // consume from a queue named Consumer.<serviceName>.VirtualTopic.<name>

            var trimmedName = trimPrefix(name, TOPIC_PREFIX);
            dest = isProducer ?
                    new DestinationInfo(DestinationType.TOPIC, f("VirtualTopic.{}", trimmedName)) :
                    new DestinationInfo(DestinationType.QUEUE, f("Consumer.{}.VirtualTopic.{}", serviceName, trimmedName));

        } else if (name.startsWith(QUEUE_PREFIX)) {

            var trimmedName = trimPrefix(name, QUEUE_PREFIX);
            dest = new DestinationInfo(DestinationType.QUEUE, trimmedName);

        } else if (name.startsWith(DYNAMIC_PREFIX)) {

            var trimmedName = trimPrefix(name, DYNAMIC_PREFIX);
            dest = new DestinationInfo(DestinationType.TOPIC, trimmedName);

        } else {

            LOG.error("Unexpected JMS configuration. A {} destination should start with '{}', '{}', '{}', or '{}' but was: '{}'",
                    isProducer ? "producer" : "consumer",
                    DestinationIdentifier.FIXED_TOPIC_PREFIX,
                    DestinationIdentifier.TOPIC_PREFIX,
                    DestinationIdentifier.QUEUE_PREFIX,
                    DestinationIdentifier.DYNAMIC_PREFIX,
                    name);
            return Optional.empty();
        }

        return Optional.of(dest);
    }

    private static String trimPrefix(String name, String prefix) {
        return name.substring(prefix.length());
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

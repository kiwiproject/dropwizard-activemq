package org.kiwiproject.dropwizard.activemq.internal;

import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.base.KiwiStrings.f;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
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

    public enum ActorType {
        PRODUCER, CONSUMER;

        public boolean isProducer() {
            return this == PRODUCER;
        }
    }

    public enum DestinationType {
        QUEUE, TOPIC
    }

    /**
     * Evaluate a destination name and return a {@link DestinationInfo}.
     * <p>
     * Note that {@code actorType} and {@code serviceName} are only used for virtual topics (the name
     * starts with {@code "topic:"}). The {@code serviceName} is only required in that case.
     * @param name        the destination name to evaluate
     * @param actorType   whether this is a producer or consumer; determines if the evaluation should be performed
     *                    from a Producer's perspective, otherwise the evaluation is performed from a Consumer's perspective
     * @param serviceName the service/application name
     *
     * @return an Optional that may contain a DestinationInfo
     */
    public static Optional<DestinationInfo> evaluateDestinationName(String name, ActorType actorType, String serviceName) {
        checkArgumentNotBlank(name, "destination name must not be blank");

        if (name.startsWith(FIXED_TOPIC_PREFIX)) {  // normal JMS topic
            var trimmedName = trimPrefix(name, FIXED_TOPIC_PREFIX);
            return Optional.of(new DestinationInfo(DestinationType.TOPIC, trimmedName));
        }

        var isProducer = actorType.isProducer(); 
        
        if (name.startsWith(TOPIC_PREFIX)) {  // this is an ActiveMQ virtual topic
            checkArgumentNotNull(actorType, "actorType must not be null");
            checkArgumentNotBlank(serviceName, "serviceName must not be blank for virtual topics ('topic:' prefix)");

            // Producers produce to VirtualTopic.<name> while consumers
            // consume from a queue named Consumer.<serviceName>.VirtualTopic.<name>

            var trimmedName = trimPrefix(name, TOPIC_PREFIX);            
            var dest = isProducer ?
                    new DestinationInfo(DestinationType.TOPIC, f("VirtualTopic.{}", trimmedName)) :
                    new DestinationInfo(DestinationType.QUEUE, f("Consumer.{}.VirtualTopic.{}", serviceName, trimmedName));
            return Optional.of(dest);
        }

        if (name.startsWith(QUEUE_PREFIX)) {
            var trimmedName = trimPrefix(name, QUEUE_PREFIX);
            return Optional.of(new DestinationInfo(DestinationType.QUEUE, trimmedName));
        }

        if (name.startsWith(DYNAMIC_PREFIX)) {
            var trimmedName = trimPrefix(name, DYNAMIC_PREFIX);

            // Note we use a topic here! This means that dynamic destination
            // strings can omit the "topic://" prefix for topics, but they
            // must always include the "queue://" prefix for queues.

            return Optional.of(new DestinationInfo(DestinationType.TOPIC, trimmedName));
        }

        LOG.error("Unexpected JMS configuration. A {} destination should start with '{}', '{}', '{}', or '{}' but was: '{}'",
                isProducer ? "producer" : "consumer",
                DestinationIdentifier.FIXED_TOPIC_PREFIX,
                DestinationIdentifier.TOPIC_PREFIX,
                DestinationIdentifier.QUEUE_PREFIX,
                DestinationIdentifier.DYNAMIC_PREFIX,
                name);
        return Optional.empty();
    }

    private static String trimPrefix(String name, String prefix) {
        return name.substring(prefix.length());
    }

    @Getter
    @Setter
    @ToString
    public static class DestinationInfo {
        private DestinationType type;
        private String name;

        DestinationInfo(DestinationType type, String name) {
            this.type = requireNotNull(type);
            this.name = requireNotBlank(name);
        }
    }
}

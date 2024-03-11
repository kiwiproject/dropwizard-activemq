package org.kiwiproject.dropwizard.activemq.util;

import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import lombok.experimental.UtilityClass;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Utility class for creating dynamic destination strings.
 */
@UtilityClass
public class DynamicDestinations {

    public static final String DYNAMIC_DESTINATION_ID = "*";

    private static final String DYNAMIC_TOPIC_PREFIX = "";  // intentionally empty

    private static final String DYNAMIC_QUEUE_PREFX = "queue://";

    /**
     * Builds the dynamic destination string for a list of topics.
     *
     * @param topicNames the list of topic names without any special prefix or formatting
     * @return the formatted, comma-separated destination string
     */
    public static String buildDynamicDestination(List<String> topicNames) {
        return buildDynamicDestination(topicNames, List.of());
    }

    /**
     * Builds the dynamic destination string for a list of topics and a list of queues.
     *
     * @param topicNames the list of topic names without any special prefix or formatting
     * @param queueNames the list of queue names without any special prefix or formatting
     * @return the formatted, comma-separated destination string
     */
    public static String buildDynamicDestination(List<String> topicNames, List<String> queueNames) {
        var topics = joinNamesWithPrefix(topicNames, DYNAMIC_TOPIC_PREFIX);
        var queues = joinNamesWithPrefix(queueNames, DYNAMIC_QUEUE_PREFX);

        var destinations = Stream.of(topics, queues)
                .filter(StringUtils::isNotBlank)
                .collect(joining(","));

        if (isNotBlank(destinations)) {
            return DYNAMIC_DESTINATION_ID + ":" + destinations;
        }

        return destinations;
    }

    private static String joinNamesWithPrefix(List<String> names, String prefix) {
        return Optional.ofNullable(names)
                .orElseGet(() -> new ArrayList<String>())
                .stream()
                .filter(Objects::nonNull)
                .map(s -> prefix + s)
                .collect(joining(","));
    }
}

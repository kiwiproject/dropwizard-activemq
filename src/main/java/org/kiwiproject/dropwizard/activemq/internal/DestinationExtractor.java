package org.kiwiproject.dropwizard.activemq.internal;

import lombok.experimental.UtilityClass;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.commons.lang3.tuple.Pair;
import org.kiwiproject.dropwizard.activemq.ActiveMqMessage;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@UtilityClass
class DestinationExtractor {

    // TODO The application.group and application.user are VERY specific things, not generic...deal with later

    private static final Pattern STRIP_PREFIX = Pattern.compile(".*:(//)?");
    private static final Pattern STRIP_VIRTUAL_TOPIC = Pattern.compile(".*VirtualTopic.");
    private static final Pattern STRIP_DYNAMIC_DESTINATION_PREFIX = Pattern.compile("\\*:");
    private static final Pattern NORMALIZE_USER_GROUP = Pattern.compile("(application.group).*");
    private static final Pattern NORMALIZE_USER = Pattern.compile("(application.user).*");

    private static final String EMPTY_REPLACEMENT = "";
    private static final String FIRST_GROUP_REPLACEMENT = "$1.##";

    private static final String DELIMITER = "::";
    static final Pattern DELIMITER_SPLITTER = Pattern.compile(DELIMITER);

    static final String BYTES_MESSAGE_TYPE = ActiveMqMessage.ContentType.BYTES.convertToMessageType();

    private static final List<Pair<Pattern, String>> DESTINATION_REPLACEMENTS = List.of(
            Pair.of(STRIP_PREFIX, EMPTY_REPLACEMENT),
            Pair.of(STRIP_VIRTUAL_TOPIC, EMPTY_REPLACEMENT),
            Pair.of(STRIP_DYNAMIC_DESTINATION_PREFIX, EMPTY_REPLACEMENT),
            Pair.of(NORMALIZE_USER_GROUP, FIRST_GROUP_REPLACEMENT),
            Pair.of(NORMALIZE_USER, FIRST_GROUP_REPLACEMENT)
    );

    static List<String> simplifyDestinations(String destinationCsv) {
        return simplifyDestinations(destinationCsv.split(","));
    }

    private static List<String> simplifyDestinations(String... destinations) {
        return Stream.of(destinations)
                .map(DestinationExtractor::simplifyDestination)
                .toList();
    }

    static String simplifyDestination(ActiveMQDestination originalDestination) {
        return simplifyDestination(originalDestination.toString());
    }

    private static String simplifyDestination(String originalDestination) {
        String simpleDestination = originalDestination;

        for (Pair<Pattern, String> pair : DESTINATION_REPLACEMENTS) {
            simpleDestination = pair.getLeft().matcher(simpleDestination).replaceAll(pair.getRight());
        }

        return simpleDestination;
    }

    static String createElucidationDestination(String destination, String messageType) {
        return String.join(DELIMITER, destination, messageType);
    }
}

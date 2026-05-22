package org.kiwiproject.dropwizard.activemq.internal;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.commons.lang3.tuple.Pair;
import org.kiwiproject.dropwizard.activemq.ActiveMqMessage;
import org.kiwiproject.dropwizard.activemq.config.DestinationNormalizerConfig;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

class DestinationExtractor {

    private static final Pattern STRIP_PREFIX = Pattern.compile(".*:(//)?");
    private static final Pattern STRIP_VIRTUAL_TOPIC = Pattern.compile(".*VirtualTopic.");
    private static final Pattern STRIP_DYNAMIC_DESTINATION_PREFIX = Pattern.compile("\\*:");

    private static final String EMPTY_REPLACEMENT = "";

    private static final List<Pair<Pattern, String>> BASE_REPLACEMENTS = List.of(
            Pair.of(STRIP_PREFIX, EMPTY_REPLACEMENT),
            Pair.of(STRIP_VIRTUAL_TOPIC, EMPTY_REPLACEMENT),
            Pair.of(STRIP_DYNAMIC_DESTINATION_PREFIX, EMPTY_REPLACEMENT)
    );

    private static final String DELIMITER = "::";
    static final Pattern DELIMITER_SPLITTER = Pattern.compile(DELIMITER);

    static final String BYTES_MESSAGE_TYPE = ActiveMqMessage.ContentType.BYTES.convertToMessageType();

    private final List<Pair<Pattern, String>> destinationReplacements;

    DestinationExtractor(List<DestinationNormalizerConfig> normalizers) {
        var compiled = normalizers.stream()
                .map(n -> Pair.of(Pattern.compile(n.getPattern()), n.getReplacement()))
                .toList();

        this.destinationReplacements = Stream.concat(BASE_REPLACEMENTS.stream(), compiled.stream()).toList();
    }

    static DestinationExtractor withNoNormalizers() {
        return new DestinationExtractor(List.of());
    }

    List<String> simplifyDestinations(String destinationCsv) {
        return simplifyDestinations(destinationCsv.split(","));
    }

    private List<String> simplifyDestinations(String... destinations) {
        return Stream.of(destinations)
                .map(this::simplifyDestination)
                .toList();
    }

    String simplifyDestination(ActiveMQDestination originalDestination) {
        return simplifyDestination(originalDestination.toString());
    }

    private String simplifyDestination(String originalDestination) {
        String simpleDestination = originalDestination;

        for (Pair<Pattern, String> pair : destinationReplacements) {
            simpleDestination = pair.getLeft().matcher(simpleDestination).replaceAll(pair.getRight());
        }

        return simpleDestination;
    }

    static String createElucidationDestination(String destination, String messageType) {
        return String.join(DELIMITER, destination, messageType);
    }
}

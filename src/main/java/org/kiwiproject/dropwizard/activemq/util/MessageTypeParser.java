package org.kiwiproject.dropwizard.activemq.util;

import static com.google.common.base.Verify.verify;
import static java.util.stream.Collectors.toSet;
import static org.kiwiproject.logging.LazyLogParameterSupplier.lazy;

import com.google.common.annotations.VisibleForTesting;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.kiwiproject.json.JsonHelper;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Parses JSON messages to find the message type, which may be at the top level of be embedded within an
 * "echoed" message. An echoed message wraps the real message, such that it must be unwrapped to obtain
 * the real message.
 */
@Slf4j
public class MessageTypeParser {

    public static final String UNKNOWN_MESSAGE_TYPE = "UNKNOWN";

    // This must be a List since we attempt to find the messageType in this specific order.
    // (It could be a LinkedHashSet, but List makes the intent for it to be ordered more clear.)
    private static final List<String> MESSAGE_TYPE_PATHS = List.of(
        "messageType",
        "metaData.type",
        "echoedMessage.messageType",
        "echoedMessage.metaData.type"
    );

    private static final String ECHOED_MESSAGE_TYPE = "ECHO_MESSAGE";

    private final JsonHelper jsonHelper;

    public MessageTypeParser(JsonHelper jsonHelper) {
        this.jsonHelper = jsonHelper;
    }

    /**
     * Return an Optional wrapping the type that was found, or an empty Optional if the input
     * is malformed JSON, or is not JSON, etc.
     * <p>
     * If the input is valid JSON but contains a blank or null message type, returns an Optional
     * wrapping "UNKNOWN".
     *
     * @param maybeJson the message (expected to be JSON, but we're not sure)
     * @return an Optional containing the type that was found, or an empty Optional
     */
    public Optional<String> findTypeSafe(String maybeJson) {
        try {
            return Optional.ofNullable(findType(maybeJson));
        } catch (MessageTypeParsingException e) {
            LOG.debug("Unable to find message type in msg: '{}', sending back an empty optional (enable TRACE for details)",
                    lazy(() -> StringUtils.abbreviate(maybeJson, 50)), e);
            LOG.trace("Message content: {}", maybeJson);
        }

        return Optional.empty();
    }

    /**
     * Find the single unique message type in the given JSON in a case-insensitive manner.
     * <p>
     * Returns "UNKNOWN" if there is no messageType or if it is null or blank.
     * <p>
     * If this is an ECHO_MESSAGE then the messageType of the echoedMessage is returned.
     *
     * @param json the message JSON
     * @return the upper-cased message type
     * @throws MessageTypeParsingException if JSON is malformed, the message is not JSON, etc.
     */
    public String findType(String json) {
        Set<String> messageTypes = findTypesExcludingEcho(json);
        return extractMessageType(messageTypes, json);
    }

    private Set<String> findTypesExcludingEcho(String json) {
        return findAllTypes(json)
                .stream()
                .filter(type -> !ECHOED_MESSAGE_TYPE.equals(type))
                .collect(toSet());
    }

    /**
     * Find all message types in the given JSON message in a case-insensitive manner.
     *
     * @param json the message JSON
     * @return all message types after upper-casing them
     * @throws MessageTypeParsingException if JSON is malformed, etc.
     */
    @VisibleForTesting
    Set<String> findAllTypes(String json) {
        try {
            return MESSAGE_TYPE_PATHS.stream()
                    .map(path -> jsonHelper.getPath(json, path, String.class))
                    .filter(StringUtils::isNotBlank)
                    .map(String::toUpperCase)
                    .collect(toSet());
        } catch (Exception e) {
            throw new MessageTypeParsingException(e);
        }
    }

    @VisibleForTesting
    static String extractMessageType(Set<String> messageTypes, String json) {
        int numTypes = messageTypes.size();

        verify(numTypes <= 1, "There is more than one message type: %s in message: %s", messageTypes, json);

        if (numTypes == 1) {
            return messageTypes.iterator().next();
        }

        return UNKNOWN_MESSAGE_TYPE;
    }

    /**
     * Check if the given JSON message is an "ECHO_MESSAGE" in a case-insensitive manner.
     *
     * @param json the message JSON
     * @return {@code true} if the given JSON message is an "ECHO_MESSAGE"; {@code false} otherwise
     * @throws MessageTypeParsingException if JSON is malformed, etc.
     */
    public boolean isEchoedMessage(String json) {
        return findAllTypes(json).contains(ECHOED_MESSAGE_TYPE);
    }
}

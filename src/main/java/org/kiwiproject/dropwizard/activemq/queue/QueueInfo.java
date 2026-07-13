package org.kiwiproject.dropwizard.activemq.queue;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.nonNull;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Basic information about a queue.
 *
 * @param exists            whether the queue exists (mainly useful for queues like the Dead Letter Queue)
 * @param textMessageCount  the number of {@link jakarta.jms.TextMessage} in the queue
 * @param bytesMessageCount the number of {@link jakarta.jms.BytesMessage} in the queue
 * @param otherMessageCount the number of all other message types in the queue
 * @param messageTypeCounts a map containing the count for each message type in {@link jakarta.jms.TextMessage}s
 */
public record QueueInfo(
        boolean exists,
        int textMessageCount,
        int bytesMessageCount,
        int otherMessageCount,
        Map<String, Integer> messageTypeCounts
) {

    public QueueInfo {
        checkMessageCount(textMessageCount, "text");
        checkMessageCount(bytesMessageCount, "bytes");
        checkMessageCount(otherMessageCount, "other");
        checkArgumentNotNull(messageTypeCounts, "messageTypeCounts must not be null");
        messageTypeCounts.forEach((type, count) ->
                checkArgument(nonNull(count) && count >= 0,
                        "count for message type '%s' must be greater than or equal to zero", type));
        messageTypeCounts = Collections.unmodifiableMap(new LinkedHashMap<>(messageTypeCounts));
    }

    private static void checkMessageCount(int count, String type) {
        checkArgument(count >= 0, "%sMessageCount must be greater than or equal to zero", type);
    }

    public static QueueInfo ofExists(int textMessageCount,
                                     int bytesMessageCount,
                                     int otherMessageCount,
                                     Map<String, Integer> messageTypeCounts) {

        return new QueueInfo(true,
                textMessageCount,
                bytesMessageCount,
                otherMessageCount,
                messageTypeCounts);
    }

    public static QueueInfo ofEmpty() {
        return QueueInfo.ofExists(0, 0, 0, Map.of());
    }

    public static QueueInfo ofDoesNotExist() {
        return new QueueInfo(false, 0, 0, 0, Map.of());
    }
}

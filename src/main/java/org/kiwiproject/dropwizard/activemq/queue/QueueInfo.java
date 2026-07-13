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
        if (exists) {
            validateCounts(textMessageCount, bytesMessageCount, otherMessageCount, messageTypeCounts);
            messageTypeCounts = Collections.unmodifiableMap(new LinkedHashMap<>(messageTypeCounts));
        } else {
            validateZeroCounts(textMessageCount, bytesMessageCount, otherMessageCount, messageTypeCounts);
            messageTypeCounts = Map.of();
        }
    }

    private static void validateCounts(int textMessageCount,
                                       int bytesMessageCount,
                                       int otherMessageCount,
                                       Map<String, Integer> messageTypeCounts) {
        checkMessageCount(textMessageCount, "text");
        checkMessageCount(bytesMessageCount, "bytes");
        checkMessageCount(otherMessageCount, "other");
        checkArgumentNotNull(messageTypeCounts, "messageTypeCounts must not be null");
        messageTypeCounts.forEach(QueueInfo::checkMessageTypeCount);
        checkArgument(
                messageTypeCounts.values().stream().mapToInt(Integer::intValue).sum() <= textMessageCount,
                "sum of messageTypeCounts must not exceed textMessageCount");
    }

    private static void checkMessageCount(int count, String type) {
        checkArgument(count >= 0, "%sMessageCount must be greater than or equal to zero", type);
    }

    private static void checkMessageTypeCount(String type, Integer count) {
        checkArgument(nonNull(count) && count >= 0,
                "count for message type '%s' must be greater than or equal to zero", type);
    }

    private static void validateZeroCounts(int textMessageCount,
                                           int bytesMessageCount,
                                           int otherMessageCount,
                                           Map<String, Integer> messageTypeCounts) {
        checkArgument(textMessageCount == 0
                        && bytesMessageCount == 0
                        && otherMessageCount == 0
                        && nonNull(messageTypeCounts)
                        && messageTypeCounts.isEmpty(),
                "all message counts must be zero and messageTypeCounts must be empty when the queue does not exist");
    }

    /**
     * Create a new instance for a queue that exists.
     *
     * @param textMessageCount  the number of {@link jakarta.jms.TextMessage} in the queue
     * @param bytesMessageCount the number of {@link jakarta.jms.BytesMessage} in the queue
     * @param otherMessageCount the number of all other message types in the queue
     * @param messageTypeCounts a map containing the number of each type of text message
     * @return a new instance
     */
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

    /**
     * Create a new instance for an empty queue.
     *
     * @return a new instance with all zero counts and no message types
     */
    public static QueueInfo ofEmpty() {
        return QueueInfo.ofExists(0, 0, 0, Map.of());
    }

    /**
     * Create a new instance for a queue that does not exist.
     *
     * @return a new instance with all zero counts and no message types
     */
    public static QueueInfo ofDoesNotExist() {
        return new QueueInfo(false, 0, 0, 0, Map.of());
    }
}

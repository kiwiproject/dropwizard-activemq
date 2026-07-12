package org.kiwiproject.dropwizard.activemq.queue;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public record QueueInfo(
        boolean exists,
        int textMessageCount,
        int bytesMessageCount,
        int otherMessageCount,
        Map<String, Integer> messageTypeCounts
) {

    public QueueInfo {
        messageTypeCounts = Collections.unmodifiableMap(new LinkedHashMap<>(messageTypeCounts));
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

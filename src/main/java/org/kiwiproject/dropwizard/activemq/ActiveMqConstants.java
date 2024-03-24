package org.kiwiproject.dropwizard.activemq;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ActiveMqConstants {

    /**
     * The "bare" name of the "All Events" queue.
     */
    public static final String ALL_EVENTS_QUEUE_NAME = "all_events";

    /**
     * The prefixed name of the "All Events" queue.
     */
    public static final String ALL_EVENTS_QUEUE = "queue:" + ALL_EVENTS_QUEUE_NAME;
}

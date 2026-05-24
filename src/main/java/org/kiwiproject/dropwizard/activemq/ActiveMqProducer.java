package org.kiwiproject.dropwizard.activemq;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.kiwiproject.dropwizard.activemq.ActiveMqProducer.PayloadDestination.SPECIFIED_AND_ALL_EVENTS;
import static org.kiwiproject.dropwizard.activemq.ActiveMqProducer.PayloadDestination.SPECIFIED_ONLY;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Interface for producing JMS messages to ActiveMQ destinations.
 * <p>
 * Supports producing text messages to a specific destination, to the configured "all events" queue,
 * or to both simultaneously. Also supports producing raw byte messages via
 * {@link #produceBytesMessage(String, byte[])}.
 */
public interface ActiveMqProducer {

    /**
     * Tells whether the message sent to the {@link #produce(String, String, PayloadDestination)} method
     * should send the payload to just the specified destination (SPECIFIED_ONLY) or to both the destination
     * and the All Events queue (SPECIFIED_AND_ALL_EVENTS).
     */
    enum PayloadDestination {
        SPECIFIED_ONLY, SPECIFIED_AND_ALL_EVENTS
    }

    /**
     * Method that will only produce a message to the "All Events" queue,
     * sent as a JMS {@link javax.jms.TextMessage}.
     * <p>
     * Implementations must route to the configured all-events queue destination,
     * as returned by {@link org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig#getAllEventsQueue()}.
     *
     * @param payload the payload to produce
     */
    void produceToAllEventsQueue(String payload);

    /**
     * Method that will only produce a message to the specified destination,
     * sent as a JMS {@link javax.jms.TextMessage}.
     *
     * @param destination the destination topic, virtual topic, or queue
     * @param payload     the payload to produce
     */
    default void produceToDestination(String destination, String payload) {
        produce(destination, payload, SPECIFIED_ONLY);
    }

    /**
     * Method that will produce a message to the specified destination and to the "All Events" queue,
     * sent as a JMS {@link javax.jms.TextMessage}.
     *
     * @param destination the destination topic, virtual topic, or queue
     * @param payload     the payload to produce
     */
    default void produceToDestinationAndAllEventsQueue(String destination, String payload) {
        produce(destination, payload, SPECIFIED_AND_ALL_EVENTS);
    }

    /**
     * Produce a message as a JMS {@link javax.jms.TextMessage}.
     *
     * @param destination        the destination topic, virtual topic, or queue
     * @param payload            the payload to produce
     * @param payloadDestination whether to send the message solely to the specified destination or to the
     *                           "All Events" queue as well (as configured via {@link org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig#getAllEventsQueue()})
     */
    default void produce(String destination, String payload, PayloadDestination payloadDestination) {
        produce(destination, payload, payloadDestination, Map.of());
    }

    /**
     * Method that will only produce a message to the specified destination,
     * sent as a JMS {@link javax.jms.TextMessage}, with the specified headers.
     *
     * @param destination the destination topic, virtual topic, or queue
     * @param payload     the payload to produce
     * @param headers     headers to add to the message prior to sending it
     */
    default void produceToDestinationWithHeaders(String destination,
                                                 String payload,
                                                 Map<String, Object> headers) {
        produce(destination, payload, SPECIFIED_ONLY, headers);
    }

    /**
     * Method that will produce a message to the specified destination and to the "All Events" queue,
     * sent as a JMS {@link javax.jms.TextMessage}, with the specified headers.
     *
     * @param destination the destination topic, virtual topic, or queue
     * @param payload     the payload to produce
     * @param headers     headers to add to the message prior to sending it
     */
    default void produceToDestinationAndAllEventsWithHeaders(String destination,
                                                             String payload,
                                                             Map<String, Object> headers) {
        produce(destination, payload, SPECIFIED_AND_ALL_EVENTS, headers);
    }

    /**
     * Produce a message as a JMS {@link javax.jms.TextMessage}.
     *
     * @param destination        the destination topic, virtual topic, or queue
     * @param payload            the payload to produce
     * @param payloadDestination whether to send the message solely to the specified destination or to the
     *                           "All Events" queue as well (as configured via {@link org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig#getAllEventsQueue()})
     * @param headers            headers to add to the message prior to sending it
     */
    void produce(String destination,
                 String payload,
                 PayloadDestination payloadDestination,
                 Map<String, Object> headers);

    /**
     * Produce a message as a JMS {@link javax.jms.BytesMessage} by converting
     * the payload into bytes using UTF-8 as the character set.
     * <p>
     * Conversion from String to a byte array is done using {@link String#getBytes(String)}
     * with {@link StandardCharsets#UTF_8} as the character set.
     * <p>
     * Use {@link #produceBytesMessage(String, byte[])} if you want to provide
     * a custom byte array.
     *
     * @param destination the destination topic, virtual topic, or queue
     * @param payload     the payload to produce
     */
    default void produceBytesMessage(String destination, String payload) {
        produceBytesMessage(destination, payload.getBytes(UTF_8));
    }

    /**
     * Produce a message as a JMS {@link javax.jms.BytesMessage}.
     *
     * @param destination the destination topic, virtual topic, or queue
     * @param payload     the payload to produce
     */
    void produceBytesMessage(String destination, byte[] payload);
}

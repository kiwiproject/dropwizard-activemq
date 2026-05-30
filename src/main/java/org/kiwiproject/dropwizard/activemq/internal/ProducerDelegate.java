package org.kiwiproject.dropwizard.activemq.internal;

import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.base.KiwiStrings.f;
import static org.kiwiproject.dropwizard.activemq.ActiveMqProducer.PayloadDestination.SPECIFIED_AND_ALL_EVENTS;
import static org.kiwiproject.dropwizard.activemq.ActiveMqProducer.PayloadDestination.SPECIFIED_ONLY;
import static org.kiwiproject.dropwizard.activemq.internal.DestinationExtractor.createElucidationDestination;
import static org.kiwiproject.dropwizard.activemq.util.DynamicDestinations.DYNAMIC_DESTINATION_ID;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Strings;
import org.kiwiproject.dropwizard.activemq.ActiveMqProducer;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.elucidation.client.ElucidationClient;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import jakarta.jms.ConnectionFactory;

/**
 * Internal implementation of {@link ActiveMqProducer} that manages a map of
 * {@link Producer} instances keyed by destination name and routes produce calls
 * accordingly, including optional fan-out to the "all events" queue and
 * Elucidation event recording.
 */
@Slf4j
public class ProducerDelegate implements ActiveMqProducer {

    private static final String PRODUCER_NOT_FOUND = "Producer '{}' not found!";

    @VisibleForTesting
    final Map<String, Producer> producers = new HashMap<>();

    private final String allEventsQueue;
    private final boolean allowDynamicDestinations;
    private final Duration timeToLive;
    private final ElucidationClient<String> elucidation;
    private final String serviceName;
    private final DestinationExtractor destinationExtractor;

    public ProducerDelegate(ConnectionFactory factory,
                            List<String> destinations,
                            List<String> defaultDestinations,
                            ElucidationClient<String> elucidation,
                            String serviceName,
                            ActiveMqConfig configuration) {

        // Set this first so that other internal methods see it (putNewProducer)
        this.serviceName = requireNotBlank(serviceName);

        checkArgumentNotNull(configuration);
        this.allEventsQueue = configuration.getAllEventsQueue();
        this.allowDynamicDestinations = configuration.isAllowDynamicDestinations();
        this.timeToLive = configuration.getTimeToLive().toJavaDuration();
        this.destinationExtractor = new DestinationExtractor(configuration.getDestinationNormalizers());

        checkArgumentNotNull(factory);
        requireNotNull(destinations).forEach(destination -> putNewProducer(destination, factory));

        checkArgumentNotNull(defaultDestinations);
        if (defaultDestinations.isEmpty()) {
            if (doesNotContainAllEventsQueue()) {
                putNewDefaultProducer(allEventsQueue, factory);
            }
        } else {
            defaultDestinations.forEach(destination -> putNewDefaultProducer(destination, factory));
        }

        if (allowDynamicDestinations) {
            putNewProducer(DYNAMIC_DESTINATION_ID, factory);
        }

        this.elucidation = requireNotNull(elucidation);
    }

    @Override
    public void produceToAllEventsQueue(String payload) {
        produce(allEventsQueue, payload, SPECIFIED_ONLY);
    }

    private void putNewProducer(String destination, ConnectionFactory factory) {
        putNewProducer(destination, factory, false);
    }

    private boolean doesNotContainAllEventsQueue() {
        return !producers.containsKey(allEventsQueue);
    }

    private void putNewDefaultProducer(String destination, ConnectionFactory factory) {
        putNewProducer(destination, factory, true);
    }

    private void putNewProducer(String destination, ConnectionFactory factory, boolean isDefaultProducer) {
        var producer = new Producer(factory, destination, isDefaultProducer, serviceName, timeToLive);

        producers.put(destination, producer);
    }

    /**
     * Does this producer produce to the specified destination, which must not be a default producer?
     */
    public boolean containsDestination(String destination) {
        return producers.containsKey(destination) && !producers.get(destination).isDefaultProducer();
    }

    /**
     * Does this producer produce to the specified destination, which must be a default producer?
     */
    public boolean containsDefaultDestination(String destination) {
        return producers.containsKey(destination) && producers.get(destination).isDefaultProducer();
    }

    @Override
    public void produce(String destination,
                        String payload,
                        PayloadDestination payloadDestination,
                        Map<String, Object> headers) {

        if (isDynamicDestination(destination)) {
            produceToDynamicDestination(destination, payload, payloadDestination, headers);
        } else if (producers.containsKey(destination)) {
            produceToKnownDestination(destination, payload, payloadDestination, headers);
        } else {
            throwIllegalArgumentExceptionForUnknownDestination(destination);
        }
    }

    private boolean isDynamicDestination(String destination) {
        return allowDynamicDestinations && Strings.CS.startsWith(destination, DYNAMIC_DESTINATION_ID + ":");
    }

    private void produceToDynamicDestination(String destination,
                                             String payload,
                                             PayloadDestination payloadDestination,
                                             Map<String, Object> headers) {

        LOG.trace("Sending to dynamic destination: {} ; Payload: {}", destination, payload);

        producers.get(DYNAMIC_DESTINATION_ID).produce(payload, destination, headers);

        produceToAllEventsIfNeeded(destination, payload, payloadDestination, headers);

        recordElucidationEvent(destination, payload);
    }

    private void produceToKnownDestination(String destination,
                                           String payload,
                                           PayloadDestination payloadDestination,
                                           Map<String, Object> headers) {

        LOG.trace("Sending to destination: {} ; Payload: {}", destination, payload);

        producers.get(destination).produce(payload, headers);

        produceToAllEventsIfNeeded(destination, payload, payloadDestination, headers);

        recordElucidationEvent(destination, payload);
    }

    private void produceToAllEventsIfNeeded(String destination,
                                            String payload,
                                            PayloadDestination payloadDestination,
                                            Map<String, Object> headers) {

        if (payloadDestination == SPECIFIED_AND_ALL_EVENTS) {
            produceToDefaultDestinations(destination, payload, headers);
        }
    }

    private void produceToDefaultDestinations(String excludedDestination,
                                              String payload,
                                              Map<String, Object> headers) {

        producers.entrySet().stream()
                .filter(destinationAndProducer ->
                        destinationIsNot(excludedDestination, destinationAndProducer) &&
                                isDefaultProducer(destinationAndProducer))
                .forEach(destinationAndProducer -> {
                    destinationAndProducer.getValue().produce(payload, headers);
                    recordElucidationEvent(destinationAndProducer.getKey(), payload);
                });
    }

    private static boolean destinationIsNot(String destination, Entry<String, Producer> destinationAndProducer) {
        var key = destinationAndProducer.getKey();

        return !key.equals(DYNAMIC_DESTINATION_ID) && !key.endsWith(destination);
    }

    private static boolean isDefaultProducer(Map.Entry<String, Producer> destinationAndProducer) {
        return destinationAndProducer.getValue().isDefaultProducer();
    }

    @Override
    public void produceBytesMessage(String destination, byte[] payload) {
        LOG.trace("Sending bytes message to {}", destination);

        if (producers.containsKey(destination)) {
            producers.get(destination).produceBytesMessage(payload);

            recordElucidationEvent(destination, DestinationExtractor.BYTES_MESSAGE_TYPE);
        } else {
            throwIllegalArgumentExceptionForUnknownDestination(destination);
        }
    }

    private void recordElucidationEvent(String destination, String payload) {
        destinationExtractor.simplifyDestinations(destination).stream()
                .map(dest -> createElucidationDestination(dest, payload))
                .forEach(event -> elucidation.recordNewEvent(event).whenComplete(ElucidationLogger::logResult));
    }

    private static void throwIllegalArgumentExceptionForUnknownDestination(String destination) {
        var message = f(PRODUCER_NOT_FOUND, destination);
        LOG.error(message);

        throw new IllegalArgumentException(message);
    }
}

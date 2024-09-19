package org.kiwiproject.dropwizard.activemq.internal;

import static org.apache.commons.lang3.StringUtils.startsWith;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.base.KiwiStrings.f;
import static org.kiwiproject.dropwizard.activemq.ActiveMqConstants.ALL_EVENTS_QUEUE;
import static org.kiwiproject.dropwizard.activemq.ActiveMqProducer.PayloadDestination.SPECIFIED_AND_ALL_EVENTS;
import static org.kiwiproject.dropwizard.activemq.internal.DestinationExtractor.createElucidationDestination;
import static org.kiwiproject.dropwizard.activemq.internal.DestinationExtractor.simplifyDestinations;
import static org.kiwiproject.dropwizard.activemq.util.DynamicDestinations.DYNAMIC_DESTINATION_ID;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.kiwiproject.dropwizard.activemq.ActiveMqProducer;
import org.kiwiproject.elucidation.client.ElucidationClient;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.jms.ConnectionFactory;

@Slf4j
public class ProducerDelegate implements ActiveMqProducer {

    private static final String PRODUCER_NOT_FOUND = "Producer '{}' not found!";

    @VisibleForTesting
    final Map<String, Producer> producers = new HashMap<>();

    private final boolean allowDynamicDestinations;
    private final Duration timeToLive;
    private final ElucidationClient<String> elucidation;
    private final String serviceName;

    public ProducerDelegate(ConnectionFactory factory,
                            List<String> destinations,
                            List<String> defaultDestinations,
                            boolean allowDynamicDestinations,
                            Duration timeToLive,
                            ElucidationClient<String> elucidation,
                            String serviceName) {

        // Set this first so that other internal methods see it (putNewProducer)
        this.serviceName = requireNotBlank(serviceName);

        this.allowDynamicDestinations = allowDynamicDestinations;
        this.timeToLive = requireNotNull(timeToLive);

        checkArgumentNotNull(factory);
        requireNotNull(destinations).forEach(destination -> putNewProducer(destination, factory));

        checkArgumentNotNull(defaultDestinations);
        if (defaultDestinations.isEmpty()) {
            if (doesNotContainAllEventsQueue()) {
                putNewDefaultProducer(ALL_EVENTS_QUEUE, factory);
            }
        } else {
            defaultDestinations.forEach(destination -> putNewDefaultProducer(destination, factory));
        }

        if (allowDynamicDestinations) {
            putNewProducer(DYNAMIC_DESTINATION_ID, factory);
        }

        this.elucidation = requireNotNull(elucidation);
    }

    private void putNewProducer(String destination, ConnectionFactory factory) {
        putNewProducer(destination, factory, false);
    }

    private boolean doesNotContainAllEventsQueue() {
        return !producers.containsKey(ALL_EVENTS_QUEUE);
    }

    private void putNewDefaultProducer(String destination, ConnectionFactory factory) {
        putNewProducer(destination, factory, true);
    }

    private void putNewProducer(String destination, ConnectionFactory factory, boolean isDefaultProducer) {
        var producer = new Producer(factory, destination, isDefaultProducer, serviceName);
        producer.setTimeToLive(timeToLive);

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
        return allowDynamicDestinations && startsWith(destination, DYNAMIC_DESTINATION_ID + ":");
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
        simplifyDestinations(destination).stream()
                .map(dest -> createElucidationDestination(dest, payload))
                .forEach(event -> elucidation.recordNewEvent(event).whenComplete(ElucidationLogger::logResult));
    }

    private static void throwIllegalArgumentExceptionForUnknownDestination(String destination) {
        var message = f(PRODUCER_NOT_FOUND, destination);
        LOG.error(message);

        throw new IllegalArgumentException(message);
    }
}

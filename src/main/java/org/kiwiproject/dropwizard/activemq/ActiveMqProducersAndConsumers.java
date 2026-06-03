package org.kiwiproject.dropwizard.activemq;

import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.dropwizard.activemq.internal.ProducerDelegate;

import java.util.Optional;
import java.util.Set;

/**
 * Defines a contract for starting ActiveMQ consumers and producers.
 */
public interface ActiveMqProducersAndConsumers {

    /**
     * Starts consuming from configured consumer destinations when
     * {@link ActiveMqConfig#isAutoRegisterConsumers()} is {@code true}.
     * <p>
     * Incoming messages will be passed to the consumer delegate.
     *
     * @param consumerDelegate the consumer that should process received messages
     * @return this instance, for fluent method-chaining
     */
    ActiveMqProducersAndConsumers startConsumers(ActiveMqConsumer consumerDelegate);

    /**
     * Starts consuming from the given destinations.
     * <p>
     * Incoming messages will be passed to the consumer delegate.
     *
     * @param consumerDelegate the consumer that should process received messages
     * @param destinations     the explicit destinations to consume
     * @return this instance, for fluent method-chaining
     * @throws IllegalArgumentException if no destinations are specified
     */
    ActiveMqProducersAndConsumers startConsumer(ActiveMqConsumer consumerDelegate, String... destinations);

    /**
     * Instantiates a producer for the configured destination(s).
     * <p>
     * Note that if you are using fluent method-chaining, this is a terminal method since it
     * must return the {@link ActiveMqProducer} for use by the caller.
     *
     * @return a new producer instance
     * @see ProducerDelegate
     */
    ActiveMqProducer startProducers();

    /**
     * Get the names of the destinations whose messages are being consumed.
     *
     * @return a set containing the names of the destinations that were initialized for consumers
     */
    Set<String> getInitializedConsumers();

    /**
     * Check whether any consumer for the given destination is actively consuming messages.
     * <p>
     * Returns {@code false} if no consumer has been started for the destination, or if a consumer
     * was registered but its thread has not yet started or has since terminated.
     *
     * @param destination the destination to check
     * @return true if at least one consumer for the destination is currently consuming messages
     */
    boolean isConsumerConsuming(String destination);

    /**
     * Get the {@link ActiveMqProducer} if {@link #startProducers()} has been called.
     * Otherwise, return empty Optional.
     * <p>
     * Generally you won't need this since {@link #startProducers()} returns the
     * producer instance.
     *
     * @return an Optional that will contain the {@link ActiveMqProducer} returned by
     * {@link #startProducers()}, or an empty Optional if no producer has been started.
     */
    Optional<ActiveMqProducer> getActiveMqProducer();
}

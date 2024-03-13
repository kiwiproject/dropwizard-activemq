package org.kiwiproject.dropwizard.activemq.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Your {@link io.dropwizard.core.Configuration} class should implement this interface
 * when you need to connect to an ActiveMQ server.
 * <p>
 * This extends {@link ElucidationConfigured} because this library reports message consumption
 * and production to an Elucidation server (unless disabled).
 */
public interface ActiveMqConfigured extends ElucidationConfigured {

    /**
     * The {@link ActiveMqConfig} that will be used to configure the ActiveMQ connection.
     * <p>
     * The configuration property (e.g., in YAML) must be named according to the value in
     * the {@link JsonProperty} annotation.
     */
    @JsonProperty("activeMq")
    ActiveMqConfig getActiveMqConfig();
}

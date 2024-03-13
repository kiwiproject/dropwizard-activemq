package org.kiwiproject.dropwizard.activemq.config;

import com.google.common.annotations.Beta;

import org.kiwiproject.config.provider.ElucidationConfigProvider;
import org.kiwiproject.jersey.client.ServiceIdentifier;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Your {@link io.dropwizard.core.Configuration} class should implement this interface
 * when you are sending message information to Elucidation.
 */
public interface ElucidationConfigured {

    /**
     * The name of the service/application that will be reported to Elucidation.
     */
    @NotBlank
    String getServiceName();

    /**
     * A ServiceIdentifier that identifies that Elucidation service in a service registry (e.g., Consul).
     */
    @NotNull
    default ServiceIdentifier getElucidationService() {
        return ServiceIdentifier.of("elicidation-service");
    }

    /**
     * Whether Elucidation is enabled.
     */
    default boolean isElucidationEnabled() {
        return isElucidationEnabled(ElucidationConfigProvider.builder().build());
    }

    /**
     * Whether Elucidation is enabled, using the supplied {@link ElucidationConfigProvider}.
     * <p>
     * This method is provided specifically for testing purposes, and should generally not
     * be used in production code, which is why it is maked as {@link Beta}.
     */
    @Beta
    default boolean isElucidationEnabled(ElucidationConfigProvider provider) {
        return provider.isEnabled();
    }
}

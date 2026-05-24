package org.kiwiproject.dropwizard.activemq.config;

import com.google.common.annotations.Beta;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.kiwiproject.config.provider.ElucidationConfigProvider;
import org.kiwiproject.jersey.client.ServiceIdentifier;

/**
 * Your {@link io.dropwizard.core.Configuration} class should implement this interface
 * when you are sending message information to Elucidation.
 */
public interface ElucidationConfigured {

    /**
     * The name of the service/application that will be reported to Elucidation.
     *
     * @return the service name
     */
    @NotBlank
    String getServiceName();

    /**
     * A ServiceIdentifier that identifies that Elucidation service in a service registry (e.g., Consul).
     *
     * @return the {@link ServiceIdentifier} for the Elucidation service
     */
    @NotNull
    default ServiceIdentifier getElucidationService() {
        return ServiceIdentifier.of("elucidation-service");
    }

    /**
     * Whether Elucidation is enabled.
     *
     * @return {@code true} if Elucidation is enabled, otherwise {@code false}
     */
    default boolean isElucidationEnabled() {
        return isElucidationEnabled(ElucidationConfigProvider.builder().build());
    }

    /**
     * Whether Elucidation is enabled, using the supplied {@link ElucidationConfigProvider}.
     * <p>
     * This method is provided specifically for testing purposes, and should generally not
     * be used in production code, which is why it is marked as {@link Beta}.
     *
     * @param provider the {@link ElucidationConfigProvider} to use
     * @return {@code true} if Elucidation is enabled, otherwise {@code false}
     */
    @Beta
    default boolean isElucidationEnabled(ElucidationConfigProvider provider) {
        return provider.isEnabled();
    }
}

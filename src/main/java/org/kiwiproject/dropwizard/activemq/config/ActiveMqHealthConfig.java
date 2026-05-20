package org.kiwiproject.dropwizard.activemq.config;

import io.dropwizard.util.Duration;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for ActiveMQ stats health checks that use Jolokia to get ActiveMQ stats.
 */
@Getter
@Setter
public class ActiveMqHealthConfig {

    /**
     * Username that Jolokia will use making HTTP requests using Basic Authentication.
     */
    @NotBlank
    private String jmxUser;

    /**
     * Password that Jolokia will use making HTTP requests using Basic Authentication.
     */
    @NotBlank
    private String jmxCred;

    @NotNull
    private List<String> ignoredDestinations = new ArrayList<>();

    private int minConsumerThreshold;

    private int maxPendingThreshold = 100;

    @NotNull
    private Duration refreshInterval = Duration.minutes(2);

    private boolean ignoreEmptyQueuesWithNoConsumers = true;

    @NotNull
    private Duration statsTimeout = Duration.seconds(10);
}

package org.kiwiproject.dropwizard.activemq.config;

import io.dropwizard.util.Duration;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Configuration for ActiveMQ stats health checks that use Jolokia to get ActiveMQ stats.
 */
@Getter
@Setter
@NoArgsConstructor
public class ActiveMqHealthConfig {

    /**
     * The default name of the ActiveMQ DLQ.
     */
    public static final String DEFAULT_DLQ_NAME = "ActiveMQ.DLQ";

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

    /**
     * The name of the dead-letter queue. Defaults to {@code "ActiveMQ.DLQ"}.
     */
    @NotBlank
    private String dlqName = DEFAULT_DLQ_NAME;

    @Builder
    public ActiveMqHealthConfig(String jmxUser,
                                String jmxCred,
                                List<String> ignoredDestinations,
                                Integer minConsumerThreshold,
                                Integer maxPendingThreshold,
                                Duration refreshInterval,
                                Boolean ignoreEmptyQueuesWithNoConsumers,
                                Duration statsTimeout,
                                String dlqName) {
        this.jmxUser = jmxUser;
        this.jmxCred = jmxCred;
        this.ignoredDestinations = Objects.requireNonNullElse(ignoredDestinations, new ArrayList<>());
        this.minConsumerThreshold = Objects.requireNonNullElse(minConsumerThreshold, 0);
        this.maxPendingThreshold = Objects.requireNonNullElse(maxPendingThreshold, 100);
        this.refreshInterval = Objects.requireNonNullElse(refreshInterval, Duration.minutes(2));
        this.ignoreEmptyQueuesWithNoConsumers = Objects.requireNonNullElse(ignoreEmptyQueuesWithNoConsumers, true);
        this.statsTimeout = Objects.requireNonNullElse(statsTimeout, Duration.seconds(10));
        this.dlqName = Objects.requireNonNullElse(dlqName, DEFAULT_DLQ_NAME);
    }
}

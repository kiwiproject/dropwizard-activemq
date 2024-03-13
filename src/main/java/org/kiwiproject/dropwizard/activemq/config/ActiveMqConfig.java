package org.kiwiproject.dropwizard.activemq.config;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

import jakarta.validation.constraints.NotBlank;

@Getter
@Setter
public class ActiveMqConfig {

    // TODO Fix all the @link and @see with FQCNs onces added

    public static final String DEFAULT_BROKER_URI = "tcp://localhost:61616";

    /**
     * The ful URI of the ActiveMQ broker, including failover, connection options, etc.
     */
    @NotBlank
    private String brokerUri = DEFAULT_BROKER_URI;

    /**
     * Whether to register a BrokerHealthCheck or not.
     */
    private boolean registerBrokerHealthCheck = true;

    /**
     * Use this to differentiate the ActiveMQ (e.g., internal-cluster, external-cluster) when
     * a service connects to more than one ActiveMQ.
     */
    private String healthCheckNamePrefix;

    /**
     * Should producer and consumer statistics health checks be enabled?
     *
     * @see ConsumerStatsHealthCheck
     * @see ProducerStatsHealthCheck
     */
    private boolean enableStatsHealthChecks = true;

    /**
     * Should Elucidation reporting be enabled?
     */
    private boolean enableElucidation = false;

    /**
     * Should all configured consumers be automatically registered when
     * {@link DropwizardActiveMq#startConsumers(ActiveMqConsumer)} is called?
     */
    private boolean autoRegisterConsumers = true;

    /**
     * List of consumer destinations, e.g., topics, queues.
     */
    private List<String> consumers = new ArrayList<>();

    /**
     * List of producer destinations, e.g., topics, queues.
     */
    private List<String> producers = new ArrayList<>();

    /**
     * List of default producer destinations.
     * <p>
     * If none set this will be defaulted to a list containing
     * {@link org.kiwiproject.dropwizard.activemq.ActiveMqConstants.ActiveMqConstants#ALL_EVENTS_QUEUE}.
     */
    private List<String> defaultProducers = new ArrayList<>();

    /**
     * Should dynamic destinations be permitted?
     * <p>
     * TODO Explain what "dynamic destinations" are!
     */
    private boolean allowDynamicDestinations;

    /**
     * Should more than one consumer be allowed for a single destination?
     */
    private boolean allowMultipleConsumersPerDestination;

    // TODO the rest...
}

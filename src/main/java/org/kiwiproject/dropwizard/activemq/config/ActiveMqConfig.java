package org.kiwiproject.dropwizard.activemq.config;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNullElse;
import static org.apache.commons.lang3.StringUtils.isBlank;

import io.dropwizard.util.Duration;
import io.dropwizard.validation.MinDuration;
import io.dropwizard.validation.ValidationMethod;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.kiwiproject.config.TlsContextConfiguration;
import org.kiwiproject.config.provider.TlsConfigProvider;
import org.kiwiproject.validation.KiwiConstraintViolations;
import org.kiwiproject.validation.KiwiValidations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Dropwizard configuration class for connecting to an ActiveMQ broker.
 * <p>
 * Covers broker URI, consumer and producer destination lists, health check settings,
 * TLS options, the "all events" queue name, and Elucidation integration.
 * Bind it in your Dropwizard {@link io.dropwizard.core.Configuration} class and implement
 * {@link ActiveMqConfigured} to make it available to the library.
 */
@Getter
@Setter
@NoArgsConstructor
@Slf4j
public class ActiveMqConfig {

    private static final long DEFAULT_CONSUMER_RECEIVE_TIMEOUT_MILLIS = 400;

    /**
     * The default bare name of the "all events" queue.
     */
    public static final String DEFAULT_ALL_EVENTS_QUEUE_NAME = "all_events";

    /**
     * The default broker URI, using the ActiveMQ SSL port.
     */
    public static final String DEFAULT_BROKER_URI = "ssl://localhost:61617";

    /**
     * The default Jolokia port.
     */
    public static final int DEFAULT_JOLOKIA_PORT = 8161;

    /**
     * The full URI of the ActiveMQ broker, including failover, connection options, etc.
     */
    @NotBlank
    private String brokerUri = DEFAULT_BROKER_URI;

    /**
     * Whether to register a BrokerHealthCheck or not.
     */
    private boolean registerBrokerHealthCheck = true;

    /**
     * The maximum duration the BrokerHealthCheck's temporary consumer will wait for the next message.
     * 
     * @see jakarta.jms.MessageConsumer#receive(long)
     */
    @NotNull
    @MinDuration(value = 10, unit = TimeUnit.MILLISECONDS)
    private Duration brokerHealthCheckConsumerReceiveTimeout = Duration.milliseconds(DEFAULT_CONSUMER_RECEIVE_TIMEOUT_MILLIS);

    /**
     * Use this to differentiate the ActiveMQ (e.g., internal-cluster, external-cluster) when
     * a service connects to more than one ActiveMQ.
     */
    private String healthCheckNamePrefix;

    /**
     * Should producer and consumer statistics health checks be enabled?
     *
     * @see org.kiwiproject.dropwizard.activemq.health.ConsumerStatsHealthCheck ConsumerStatsHealthCheck
     * @see org.kiwiproject.dropwizard.activemq.health.ProducerStatsHealthCheck ProducerStatsHealthCheck
     */
    private boolean enableStatsHealthChecks = true;

    /**
     * Should Elucidation reporting be enabled?
     */
    private boolean enableElucidation = false;

    /**
     * App-specific destination normalizers applied only when simplifying JMS destination names for
     * <a href="https://github.com/elucidation-project/elucidation">Elucidation</a> event
     * recording. They have no effect on actual JMS routing.
     * <p>
     * Each normalizer is a regex {@code pattern} + {@code replacement} pair. They are applied in
     * order, after the built-in prefix/VirtualTopic/dynamic-destination stripping. Defaults to an
     * empty list — no app-specific normalization.
     * <p>
     * If multiple normalizers can match the same destination string, they are applied in sequence —
     * the output of one becomes the input of the next. Design normalizers with non-overlapping
     * patterns to avoid unintended interactions.
     * <p>
     * Example (YAML):
     * <pre>
     *   destinationNormalizers:
     *     - pattern: "(myapp.group).*"
     *       replacement: "$1.##"
     *     - pattern: "(myapp.user).*"
     *       replacement: "$1.##"
     * </pre>
     */
    @NotNull
    @Valid
    private List<DestinationNormalizerConfig> destinationNormalizers = new ArrayList<>();

    /**
     * Should all configured consumers be automatically registered when
     * {@link org.kiwiproject.dropwizard.activemq.DropwizardActiveMq#startConsumers(org.kiwiproject.dropwizard.activemq.ActiveMqConsumer) DropwizardActiveMq#startConsumers}
     * is called?
     */
    private boolean autoRegisterConsumers = true;

    /**
     * List of consumer destinations, e.g., topics, queues.
     */
    @NotNull
    private List<String> consumers = new ArrayList<>();

    /**
     * The maximum duration a consumer will wait for the next message.
     * 
     * @see jakarta.jms.MessageConsumer#receive(long)
     */
    @NotNull
    @MinDuration(value = 10, unit = TimeUnit.MILLISECONDS)
    private Duration consumerReceiveTimeout = Duration.milliseconds(DEFAULT_CONSUMER_RECEIVE_TIMEOUT_MILLIS);

    /**
     * List of producer destinations, e.g., topics, queues.
     */
    @NotNull
    private List<String> producers = new ArrayList<>();

    /**
     * List of default producer destinations.
     * <p>
     * If none set this will be defaulted to a list containing {@link #getAllEventsQueue()}.
     */
    @NotNull
    private List<String> defaultProducers = new ArrayList<>();

    /**
     * The bare name of the "all events" queue.
     * <p>
     * Defaults to {@code "all_events"}, which produces a full destination of {@code "queue:all_events"}.
     * Use {@link #getAllEventsQueue()} to get the full destination string.
     */
    @NotBlank
    private String allEventsQueueName = DEFAULT_ALL_EVENTS_QUEUE_NAME;

    /**
     * Should dynamic destinations be permitted?
     * <p>
     * Dynamic destinations let you specify more than one destination
     * using strings like {@code "*:topic://topic-A,topic://topic-B,queue://queue-audit"}.
     * <p>
     * You can omit "topic://" for topics, so the above can be simplified
     * to just {@code "*:topic-A,topic-B,queue://queue-audit"}.
     */
    private boolean allowDynamicDestinations;

    /**
     * Should more than one consumer be allowed for a single destination?
     */
    private boolean allowMultipleConsumersPerDestination;

    /**
     * Timeout that applies to produced messages.
     * <p>
     * Defaults to {@code 1 hour}. The JMS specification default is {@code 0} (unlimited),
     * but an unlimited TTL means expired messages will never appear in the DLQ, making
     * consumer failures invisible until manually investigated. A shorter TTL ensures
     * problems surface quickly. Set to {@code 0} to disable expiry entirely.
     *
     * @see jakarta.jms.MessageProducer#setTimeToLive(long)
     */
    @NotNull
    private Duration timeToLive = Duration.hours(1);

    /**
     * Configuration used to retrieve health statistics via REST (using Jolokia) from the ActiveMQ server.
     */
    @NotNull
    @Valid
    private ActiveMqHealthConfig healthConfig = new ActiveMqHealthConfig();

    /**
     * Should DropwizardActiveMq connect to ActiveMQ only via secure connections, e.g., TLS using certificates?
     */
    private boolean useSecureActiveMQConnections = true;

    /**
     * Should DropwizardActiveMq verify the ActiveMQ broker's hostname?
     * <p>
     * By default, this is {@code true}. When set to {@code false}, {@code verifyHostName=false} is
     * appended to the broker URI for regular transport connections. For failover connections,
     * {@code nested.verifyHostName=false} is used instead. See {@link #getResolvedBrokerUri()}.
     */
    private boolean verifyActiveMQBrokerHostnames = true;

    /**
     * The port to use when connecting to the ActiveMQ Jolokia REST API.
     */
    @PositiveOrZero
    private int jolokiaPort = DEFAULT_JOLOKIA_PORT;

    /**
     * Should DropwizardActiveMq connect to the ActiveMQ Jolokia REST API only via secure connections, e.g., TLS,
     * to gather statistics?
     */
    private boolean useSecureRestConnections = true;

    /**
     * Should DropwizardActiveMq verify host names when using Jolokia REST secure connections?
     * <p>
     * By default, this is {@code true}.
     * <p>
     * The value of this option only matters if {@link #isUseSecureRestConnections()} is {@code true}.
     * Otherwise, it is ignored (because it won't be used).
     */
    private boolean verifyRestConnectionHostnames = true;

    /**
     * TLS configuration to use when connecting to the ActiveMQ message broker and/or to
     * the ActiveMQ Jolokia REST API.
     * <p>
     * Required only when using secure connections.
     * <p>
     * Uses {@link TlsConfigProvider} to provide a default {@link TlsContextConfiguration}, so it
     * will automatically use environment variables, system properties, or external configuration.
     */
    private TlsContextConfiguration tlsConfiguration =
            TlsConfigProvider.builder().build().getTlsContextConfiguration();

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private String resolvedBrokerUri;

    /**
     * Returns the full destination string for the all-events queue,
     * i.e., {@code "queue:"} + {@link #getAllEventsQueueName()}.
     */
    public String getAllEventsQueue() {
        return "queue:" + allEventsQueueName;
    }

    /**
     * Returns the broker URI with hostname verification options applied.
     * <p>
     * When {@link #isVerifyActiveMQBrokerHostnames()} is {@code true} (the default), this returns
     * {@link #getBrokerUri()} unchanged. When it is {@code false}, {@code verifyHostName=false} is
     * appended for regular transport URIs, or {@code nested.verifyHostName=false} for failover URIs.
     * <p>
     * The result is computed once and cached. This config object must not be mutated (via
     * {@link #setBrokerUri(String)} or {@link #setVerifyActiveMQBrokerHostnames(boolean)}) after
     * this method has been called, as changes will not be reflected in the cached value.
     */
    @Synchronized
    public String getResolvedBrokerUri() {
        if (isNull(resolvedBrokerUri)) {
            resolvedBrokerUri = computeResolvedBrokerUri();
        }
        return resolvedBrokerUri;
    }

    private String computeResolvedBrokerUri() {
        if (isBlank(brokerUri) || verifyActiveMQBrokerHostnames) {
            return brokerUri;
        }

        var isFailover = brokerUri.startsWith("failover:");
        var paramName = isFailover ? "nested.verifyHostName" : "verifyHostName";
        var disableParam = paramName + "=false";

        if (brokerUri.contains(disableParam)) {
            return brokerUri;
        }

        var separator = brokerUri.contains("?") ? "&" : "?";
        return brokerUri + separator + disableParam;
    }

    @ValidationMethod(message = "verifyActiveMQBrokerHostnames conflicts with verifyHostName in brokerUri")
    @SuppressWarnings({ "RedundantIfStatement", "java:S1126" })  // keep separate easier-to-read conditionals
    public boolean isVerifyActiveMQBrokerHostnamesConsistent() {
        if (isBlank(brokerUri)) {
            return true;
        }

        var isFailover = brokerUri.startsWith("failover:");
        var paramName = isFailover ? "nested.verifyHostName" : "verifyHostName";

        if (!verifyActiveMQBrokerHostnames && brokerUri.contains(paramName + "=true")) {
            return false;
        }

        if (verifyActiveMQBrokerHostnames && brokerUri.contains(paramName + "=false")) {
            return false;
        }

        return true;
    }

    /**
     * This is a validation method that checks that the configuration contains a TLS configuration if either
     * "useSecureXxxConnections" property is true. If neither is true, then we don't care if there is a TLS
     * configuration and return true to indicate valid. In other words, if either of the "useSecureXxxConnections"
     * properties is true, then we require a tlsConfiguration to be present. When using secure connections, this
     * also validates that the values in the tlsConfiguration are valid.
     * <p>
     * Note that if both "useSecureXxxConnections" properties are true, they both use the same tlsConfiguration.
     * This should be fine in most circumstances. If at some point we find this assumption no longer holds,
     * we would need to permit separate TLS configuration for the broker versus the Jolokia REST APIs.
     */
    @ValidationMethod(message = "tlsConfiguration must exist and be valid when using secure connections")
    public boolean isTlsConfigurationValid() {
        if (notUsingAnySecureConnections()) {
            return true;
        }

        if (isNull(tlsConfiguration)) {
            LOG.error("Using secure connections, but tlsConfiguration is null");
            return false;
        }

        var violations = KiwiValidations.validate(tlsConfiguration);
        var isValid = violations.isEmpty();

        if (!isValid) {
            var tlsConfigErrors = KiwiConstraintViolations.prettyCombinedErrorMessage(violations);
            LOG.error("{} errors in tlsConfiguration: {}", violations.size(), tlsConfigErrors);
        }

        return isValid;
    }

    private boolean notUsingAnySecureConnections() {
        return !useSecureActiveMQConnections && !useSecureRestConnections;
    }

    /**
     * Check secure broker URLs: if using secure ActiveMQ connections, then the broker URI should contain "ssl://".
     * If not using secure connections, then the broker URI should not contain "ssl://".
     * <p>
     * This is certainly not foolproof, but it should catch most simple configuration errors - for example,
     * saying you want secure connections but not using the "ssl" scheme, or saying you do not want secure
     * connections but using the "ssl" scheme.
     */
    @ValidationMethod(message = "must use ssl scheme only for secure connections")
    public boolean isBrokerUriForSslProbablyValid() {
        return useSecureActiveMQConnections == getBrokerUri().contains("ssl://");
    }

    /**
     * Builder constructor for programmatic construction.
     * <p>
     * Note: {@code healthCheckNamePrefix} has no default and may be {@code null}.
     */
    @Builder
    private ActiveMqConfig(String brokerUri,
                          Boolean registerBrokerHealthCheck,
                          Duration brokerHealthCheckConsumerReceiveTimeout,
                          String healthCheckNamePrefix,
                          Boolean enableStatsHealthChecks,
                          Boolean enableElucidation,
                          List<DestinationNormalizerConfig> destinationNormalizers,
                          Boolean autoRegisterConsumers,
                          List<String> consumers,
                          Duration consumerReceiveTimeout,
                          List<String> producers,
                          List<String> defaultProducers,
                          String allEventsQueueName,
                          Boolean allowDynamicDestinations,
                          Boolean allowMultipleConsumersPerDestination,
                          Duration timeToLive,
                          ActiveMqHealthConfig healthConfig,
                          Boolean useSecureActiveMQConnections,
                          Boolean verifyActiveMQBrokerHostnames,
                          Integer jolokiaPort,
                          Boolean useSecureRestConnections,
                          Boolean verifyRestConnectionHostnames,
                          TlsContextConfiguration tlsConfiguration) {
        this.brokerUri = requireNonNullElse(brokerUri, DEFAULT_BROKER_URI);
        this.registerBrokerHealthCheck = requireNonNullElse(registerBrokerHealthCheck, true);
        this.brokerHealthCheckConsumerReceiveTimeout = requireNonNullElse(
                brokerHealthCheckConsumerReceiveTimeout, Duration.milliseconds(DEFAULT_CONSUMER_RECEIVE_TIMEOUT_MILLIS));
        this.healthCheckNamePrefix = healthCheckNamePrefix;
        this.enableStatsHealthChecks = requireNonNullElse(enableStatsHealthChecks, true);
        this.enableElucidation = requireNonNullElse(enableElucidation, false);
        this.destinationNormalizers = requireNonNullElse(destinationNormalizers, new ArrayList<>());
        this.autoRegisterConsumers = requireNonNullElse(autoRegisterConsumers, true);
        this.consumers = requireNonNullElse(consumers, new ArrayList<>());
        this.consumerReceiveTimeout = requireNonNullElse(
                consumerReceiveTimeout, Duration.milliseconds(DEFAULT_CONSUMER_RECEIVE_TIMEOUT_MILLIS));
        this.producers = requireNonNullElse(producers, new ArrayList<>());
        this.defaultProducers = requireNonNullElse(defaultProducers, new ArrayList<>());
        this.allEventsQueueName = requireNonNullElse(allEventsQueueName, DEFAULT_ALL_EVENTS_QUEUE_NAME);
        this.allowDynamicDestinations = requireNonNullElse(allowDynamicDestinations, false);
        this.allowMultipleConsumersPerDestination = requireNonNullElse(allowMultipleConsumersPerDestination, false);
        this.timeToLive = requireNonNullElse(timeToLive, Duration.hours(1));
        this.healthConfig = requireNonNullElse(healthConfig, new ActiveMqHealthConfig());
        this.useSecureActiveMQConnections = requireNonNullElse(useSecureActiveMQConnections, true);
        this.verifyActiveMQBrokerHostnames = requireNonNullElse(verifyActiveMQBrokerHostnames, true);
        this.jolokiaPort = requireNonNullElse(jolokiaPort, DEFAULT_JOLOKIA_PORT);
        this.useSecureRestConnections = requireNonNullElse(useSecureRestConnections, true);
        this.verifyRestConnectionHostnames = requireNonNullElse(verifyRestConnectionHostnames, true);
        this.tlsConfiguration = requireNonNullElse(
                tlsConfiguration, TlsConfigProvider.builder().build().getTlsContextConfiguration());
    }
}

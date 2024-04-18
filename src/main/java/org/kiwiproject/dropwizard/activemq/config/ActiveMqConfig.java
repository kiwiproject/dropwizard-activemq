package org.kiwiproject.dropwizard.activemq.config;

import static java.util.Objects.isNull;

import io.dropwizard.util.Duration;
import io.dropwizard.validation.ValidationMethod;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.kiwiproject.config.TlsContextConfiguration;
import org.kiwiproject.config.provider.TlsConfigProvider;
import org.kiwiproject.validation.KiwiConstraintViolations;
import org.kiwiproject.validation.KiwiValidations;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Slf4j
public class ActiveMqConfig {

    public static final String DEFAULT_BROKER_URI = "tcp://localhost:61616";

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
     * Should all configured consumers be automatically registered when
     * {@link org.kiwiproject.dropwizard.activemq.DropwizardActiveMq#startConsumers(org.kiwiproject.dropwizard.activemq.ActiveMqConsumer) DropwizardActiveMq#startConsumers}
     * is called?
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
     * {@link org.kiwiproject.dropwizard.activemq.ActiveMqConstants#ALL_EVENTS_QUEUE ALL_EVENTS_QUEUE}.
     */
    private List<String> defaultProducers = new ArrayList<>();

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
     *
     * @see javax.jms.MessageProducer#setTimeToLive(long)
     */
    private Duration timeToLive = Duration.days(7);

    /**
     * Configuration used to retrieve health statistics via REST (using Jolokia) from the ActiveMQ server.
     */
    @NotNull
    private ActiveMqHealthConfig healthConfig = new ActiveMqHealthConfig();

    /**
     * Should DropwizardActiveMq connect to ActiveMQ only via secure connections, e.g., TLS using certificates?
     */
    private boolean useSecureActiveMQConnections = true;

    /**
     * Should DropwizardActiveMq connect to the ActiveMQ REST API only via secure connections, e.g., TLS,
     * to gather statistics?
     */
    private boolean useSecureRestConnections = true;

    /**
     * Should DropwizardActiveMq verify host names when using REST secure connections?
     * <p>
     * The value of this option only matters if {@link #isUseSecureRestConnections()} is {@code true}.
     * Otherwise, it is ignored (because it won't be used).
     */
    private boolean verifyRestConnectionHostnames;

    /**
     * TLS configuration to use when connecting to the ActiveMQ message broker and/or to
     * the ActiveMQ REST API.
     * <p>
     * Required only when using secure connections.
     * <p>
     * Uses {@link TlsConfigProvider} to provide a default {@link TlsContextConfiguration}, so it
     * will automatically use environment variables, system properties, or external configuration.
     */
    private TlsContextConfiguration tlsConfiguration =
            TlsConfigProvider.builder().build().getTlsContextConfiguration();

    /**
     * This is a validation method that checks that the configuration contains a TLS configuration if either
     * "useSecureXxxConnections" property is true. If neither is true, then we don't care if there is a TLS
     * configuration and return true to indicate valid. In other words, if either of the "useSecureXxxConnections"
     * properties is true, then we require a tlsConfiguration to be present. When using secure connections, this
     * also validates that the values in the tlsConfiguration are valid.
     * <p>
     * Note that if both "useSecureXxxConnections" properties are true, they both use the same tlsConfiguration.
     * This should be fine in most circumstances. If at some point we find this assumption no longer holds,
     * we would need to permit separate TLS configuration for the broker versus the REST APIs.
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
     * This is certainly not foolproof, but it should catch most simple configuration errors, such as saying
     * you want to use secure connections, but you're not using the "ssl" scheme in the broker URI, or saying you
     * do not want to use secure connections, but you are using the "ssl" scheme in the broker URI.
     */
    @ValidationMethod(message = "must use ssk scheme only for secure connections")
    public boolean isBrokerUriForSslProbablyValid() {
        return useSecureActiveMQConnections == getBrokerUri().contains("ssl://");
    }
}

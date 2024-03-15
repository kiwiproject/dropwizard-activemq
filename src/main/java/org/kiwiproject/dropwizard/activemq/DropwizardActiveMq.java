package org.kiwiproject.dropwizard.activemq;

import static java.util.Objects.isNull;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;

import lombok.Builder;

import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfigured;
import org.kiwiproject.dropwizard.activemq.internal.ElucidationConfigurator;
import org.kiwiproject.elucidation.client.ElucidationRecorder;
import org.kiwiproject.elucidation.common.model.ConnectionEvent;
import org.kiwiproject.jersey.client.RegistryAwareClient;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import io.dropwizard.core.setup.Environment;

/**
 * The main entry point to initialize the library for consuming and/or producing
 * JMS messages via ActiveMQ.
 * <p>
 * Use the {@link #builder()} to construct a new instance, then call methods to
 * start consumers and/or producers.
 */
public class DropwizardActiveMq<C extends ActiveMqConfigured> {

    private final Environment environment;
    private final C configuration;
    private final ActiveMqConfig activeMqConfig;
    private final PooledConnectionFactory factory;
    private final boolean autoStartConsumers;
    private final List<String> consumerDestinations;
    private final boolean allowMultipleConsumersPerDestination;
    private final List<String> producerDestinations;
    private final List<String> defaultProducerDestinations;
    private final String healthCheckNamePrefix;
    private final boolean allowDynamicDestinations;
    private final Duration timeToLive;

    // Elucidation-related fields
    private ElucidationContext elucidationContext;
    private ElucidationRecorder eventRecorder;
    private Function<String, Optional<ConnectionEvent>> consumingTextMessageEventFactory;
    private Function<String, Optional<ConnectionEvent>> producingTextMessageEventFactory;

    // This is used to track consumer destinations; it is used in
    // conjunction with allowMultipleConsumersPerDestination.
    private final Set<String> initializedConsumers = ConcurrentHashMap.newKeySet();

    private ActiveMqProducer activeMqProducer;

    @Builder
    protected DropwizardActiveMq(Environment environment,
                                 C configuration,
                                 RegistryAwareClient registryAwareClient,
                                 ActiveMqHelper activeMqHelper) {

        this.environment = requireNotNull(environment);
        this.configuration = requireNotNull(configuration);
        this.activeMqConfig = requireNotNull(configuration.getActiveMqConfig());

        var safeActiveMqHelper = isNull(activeMqHelper) ? new ActiveMqHelper() : activeMqHelper;
        factory = safeActiveMqHelper.newPooledConnectionFactory(activeMqConfig);
        autoStartConsumers = activeMqConfig.isAutoRegisterConsumers();
        consumerDestinations = activeMqConfig.getConsumers();
        allowMultipleConsumersPerDestination = activeMqConfig.isAllowMultipleConsumersPerDestination();
        producerDestinations = activeMqConfig.getProducers();
        defaultProducerDestinations = activeMqConfig.getDefaultProducers();
        healthCheckNamePrefix = activeMqConfig.getHealthCheckNamePrefix();
        allowDynamicDestinations = activeMqConfig.isAllowDynamicDestinations();
        timeToLive = activeMqConfig.getTimeToLive().toJavaDuration();

        if (configuration.isElucidationEnabled()) {
            checkArgumentNotNull(registryAwareClient,
                    "registryAwareClient must not be null when Elucidation is enabled");
            elucidationContext = ElucidationConfigurator.configure(configuration, registryAwareClient);
            eventRecorder = elucidationContext.getEventRecorder();
            consumingTextMessageEventFactory = elucidationContext.getConsumingTextMessageEventFactory();
            producingTextMessageEventFactory = elucidationContext.getProducingTextMessageEventFactory();
        }

        if (activeMqConfig.isEnableStatsHealthChecks()) {
            registerStatsHealthChecks();
        }

        registerBrokerHealthCheckIfNecessary();
        manageActiveMQConnectionFactory();
    }

    private void registerStatsHealthChecks() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'registerStatsHealthChecks'");
    }

    private void registerBrokerHealthCheckIfNecessary() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'registerBrokerHealthCheckIfNecessary'");
    }

    private void manageActiveMQConnectionFactory() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'manageActiveMQConnectionFactory'");
    }

    // TODO finish it...
}

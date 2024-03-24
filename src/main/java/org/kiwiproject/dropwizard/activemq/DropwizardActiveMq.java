package org.kiwiproject.dropwizard.activemq;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.base.KiwiStrings.f;
import static org.kiwiproject.collect.KiwiArrays.isNotNullOrEmpty;
import static org.kiwiproject.collect.KiwiLists.isNotNullOrEmpty;

import io.dropwizard.core.setup.Environment;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfigured;
import org.kiwiproject.dropwizard.activemq.health.ConsumerStatsHealthCheck;
import org.kiwiproject.dropwizard.activemq.health.ProducerStatsHealthCheck;
import org.kiwiproject.dropwizard.activemq.internal.BrokerHealthCheck;
import org.kiwiproject.dropwizard.activemq.internal.Consumer;
import org.kiwiproject.dropwizard.activemq.internal.ElucidationConfigurator;
import org.kiwiproject.dropwizard.activemq.internal.ProducerDelegate;
import org.kiwiproject.dropwizard.lifecycle.KiwiDropwizardLifecycles;
import org.kiwiproject.elucidation.client.ElucidationClient;
import org.kiwiproject.elucidation.client.ElucidationRecorder;
import org.kiwiproject.elucidation.common.model.ConnectionEvent;
import org.kiwiproject.jersey.client.RegistryAwareClient;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * The main entry point to initialize the library for consuming and/or producing
 * JMS messages via ActiveMQ.
 * <p>
 * Use the {@link #builder()} to construct a new instance, then call methods to
 * start consumers and/or producers.
 */
@Slf4j
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
        var healthCheckRegistry = environment.healthChecks();

        if (isNotNullOrEmpty(producerDestinations)) {
            var name = healthCheckName(ProducerStatsHealthCheck.DEFAULT_NAME);
            var healthCheck = new ProducerStatsHealthCheck<>(configuration);
            healthCheckRegistry.register(name, healthCheck);
        }

        if (isNotNullOrEmpty(consumerDestinations)) {
            var name = healthCheckName(ConsumerStatsHealthCheck.DEFAULT_NAME);
            var healthCheck = new ConsumerStatsHealthCheck<>(configuration);
            healthCheckRegistry.register(name, healthCheck);
        }
    }

    private String healthCheckName(String title) {
        return healthCheckName(healthCheckNamePrefix, title);
    }

    private static String healthCheckName(String prefix, String title) {
        return f("{} {}", nullToEmpty(prefix), title).strip();
    }

    private void registerBrokerHealthCheckIfNecessary() {
        if (!activeMqConfig.isRegisterBrokerHealthCheck()) {
            LOG.info("Skip registration of BrokerHealthCheck for {}", healthCheckNamePrefix);
            return;
        }

        requireNonNull(environment);
        requireNonNull(factory);

        var name = BrokerHealthCheck.createHealthCheckNameWithPrefix(healthCheckNamePrefix);
        var healthCheck = new BrokerHealthCheck(name, factory, configuration.getServiceName());
        environment.healthChecks().register(name, healthCheck);
    }

    private void manageActiveMQConnectionFactory() {
        requireNonNull(environment);
        requireNonNull(factory);

        KiwiDropwizardLifecycles.manage(environment.lifecycle(), factory::start, factory::stop);
    }

    /**
     * Starts consuming from configured consumer desintations when {@link ActiveMqConfig#isAutoRegisterConsumers()}
     * is {@code true}.
     * <p>
     * Incoming messages will be passed to the consumer delegate.
     *
     * @param consumerDelegate the consumer that should process received messages
     * @return this instance, for fluent method-chaining
     */
    public DropwizardActiveMq<C> startConsumers(ActiveMqConsumer consumerDelegate) {
        checkArgumentNotNull(consumerDelegate);
        requireNonNull(consumerDestinations);

        if (autoStartConsumers) {
            consumerDestinations.forEach(destination -> startConsumer(destination, consumerDelegate));
        }

        return this;
    }

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
    public DropwizardActiveMq<C> startConsumer(ActiveMqConsumer consumerDelegate, String... destinations) {
        checkArgument(isNotNullOrEmpty(destinations),
                "No destinations specified, which would result in no messages being consumed!");

        Stream.of(destinations).forEach(dest -> startConsumer(dest, consumerDelegate));

        return this;
    }

    private void startConsumer(String destination, ActiveMqConsumer consumerDelegate) {
        requireNonNull(factory);
        requireNonNull(environment);

        checkArgumentNotBlank(destination);
        checkArgumentNotNull(consumerDelegate);

        checkForExistingConsumer(destination);

        var consumer = new Consumer(
                factory,
                destination,
                consumerDelegate,
                ElucidationClient.of(eventRecorder, consumingTextMessageEventFactory),
                configuration.getServiceName()
        );

        addConsumer(destination);

        environment.lifecycle().manage(consumer);
        environment.healthChecks().register("consumer-" + destination, consumer.getHealthCheck());
    }

    private void checkForExistingConsumer(String destination) {
        checkArgumentNotBlank(destination);

        if (!allowMultipleConsumersPerDestination && initializedConsumers.contains(destination)) {
            throw new IllegalStateException(f("A consumer for destination '{}' already exists", destination));
        }
    }

    private void addConsumer(String destination) {
        LOG.debug("Adding initialized consumer for destination: {}", destination);
        initializedConsumers.add(destination);
    }

    /**
     * Instantiates a producer for the configured destination(s).
     * <p>
     * Note that if you are using fluent method-chaining, this is a terminal method since it
     * must return the {@link ActiveMqProducer} for use by the caller.
     *
     * @return a new producer instance
     * @see ProducerDelegate
     */
    public ActiveMqProducer startProducers() {
        requireNonNull(factory);
        requireNonNull(producerDestinations);
        requireNonNull(defaultProducerDestinations);

        activeMqProducer = new ProducerDelegate(factory,
                producerDestinations,
                defaultProducerDestinations,
                allowDynamicDestinations,
                timeToLive,
                ElucidationClient.of(eventRecorder, producingTextMessageEventFactory),
                configuration.getServiceName());

        return activeMqProducer;
    }

    /**
     * @return a set containing the names of the destinations that were initialized for consumers
     */
    public Set<String> getInitializedConsumers() {
        return Set.copyOf(initializedConsumers);
    }

    /**
     * @return an Optional that will contain the {@link ActiveMqProducer} returned by {@link #startProducers()}, or
     * an empty Optional if no producer has been started. Generally you won't need this since {@link #startProducers()}
     * returns the producer instance.
     */
    public Optional<ActiveMqProducer> getActiveMqProducer() {
        return Optional.ofNullable(activeMqProducer);
    }

    /**
     * @return an Optional containing the {@link ElucidationContext} if elucidation is enabled.
     * Generally you won't need this.
     */
    public Optional<ElucidationContext> getElucidationContext() {
        return Optional.ofNullable(elucidationContext);
    }
}

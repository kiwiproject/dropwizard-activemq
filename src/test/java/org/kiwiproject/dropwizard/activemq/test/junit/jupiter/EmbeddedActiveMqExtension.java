package org.kiwiproject.dropwizard.activemq.test.junit.jupiter;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.jms.ConnectionFactory;
import java.time.Duration;
import java.util.function.Consumer;

/**
 * Creates an embedded ActiveMQ broker for use in JUnit 5 tests.
 * <p>
 * Example usage:
 * <pre>
 *  {@literal @}RegisterExtension
 *   final EmbeddedActiveMqExtension broker = new EmbeddedActiveMqExtension();
 * </pre>
 * <p>
 * This extension sets up and tears down the broker before and after EACH test, not once
 * for a test class like you might expect (or want). There are two main reasons for this:
 * <ol>
 * <li>This was how it worked when it was originally implemented as a JUnit 4 rule</li>
 * <li>
 * We were unable to get the {@link org.junit.jupiter.api.extension.BeforeAllCallback}
 * and {@link org.junit.jupiter.api.extension.AfterAllCallback} callbacks to fire. Putting
 * the exact same code that is in the {@code beforeEach} and {@code afterEach} methods
 * in the {@code beforeAll} and {@code afterAll} methods, respectively, did not work.
 * For some reason they "refused" to call the {@link BrokerService#start()} and
 * {@link BrokerService#stop()} methods. As far as we could tell, it seemed to have
 * something to do with the timing of registering the broker with the ActiveMQ internal
 * {@link org.apache.activemq.broker.BrokerRegistry}.
 * </li>
 * </ol>
 */
@Slf4j
public class EmbeddedActiveMqExtension implements BeforeEachCallback, AfterEachCallback {

    private final BrokerService service;

    public EmbeddedActiveMqExtension() {
        service = new BrokerService();
        service.setBrokerName("embedded-broker");
        service.setEnableStatistics(false);
        service.setPersistent(false);
        service.setUseJmx(false);
        service.setUseShutdownHook(false);
    }

    public static EmbeddedActiveMqExtension withConfiguration(Consumer<BrokerService> configurer) {
        var extension = new EmbeddedActiveMqExtension();
        configurer.accept(extension.service);
        return extension;
    }

    @Override
    public void beforeEach(@NonNull ExtensionContext context) {
        try {
            LOG.trace("starting embedded broker");
            service.start();
        } catch (Exception e) {
            throw new RuntimeException("Could not start embedded broker!", e);
        }

        var timeoutMillis = Duration.ofSeconds(10).toMillis();

        if (!service.waitUntilStarted(timeoutMillis)) {
            throw new RuntimeException("The broker isn't started (timeout)");
        }
    }

    @Override
    public void afterEach(@NonNull ExtensionContext context) {
        if (!service.isStopped()) {
            try {
                LOG.trace("stopping embedded broker");
                service.stop();
            } catch (Exception e) {
                throw new RuntimeException("Could not stop embedded broker!", e);
            }
        }

        // BrokerService does not provide a waitUntilStopped(timeout) overload,
        // so this blocks until the broker is fully stopped.
        service.waitUntilStopped();
    }

    /**
     * Use this in a test to get a new {@link ConnectionFactory} for the embedded broker.
     * <p>
     * Once you have the factory, you need to create a {@link javax.jms.Connection}, for example,
     * using {@link ConnectionFactory#createConnection()}, and then create a {@link javax.jms.Session}.
     *
     * @return a JMS connection factory
     */
    public ConnectionFactory newConnectionFactory() {
        var factory = new ActiveMQConnectionFactory();
        factory.setBrokerURL(service.getVmConnectorURI().toString());
        return factory;
    }

    /**
     * Use this in a test to get a new {@link PooledConnectionFactory} for the embedded broker.
     * <p>
     * Once you have the factory, you need to create a {@link javax.jms.Connection}, for example,
     * using {@link ConnectionFactory#createConnection()}, and then create a {@link javax.jms.Session}.
     *
     * @return an ActiveMQ pooled connection factory
     */
    public PooledConnectionFactory newPooledConnectionFactory() {
        var factory = new ActiveMQConnectionFactory();
        factory.setBrokerURL(service.getVmConnectorURI().toString());

        var pooledFactory = new PooledConnectionFactory();
        pooledFactory.setConnectionFactory(factory);

        return pooledFactory;
    }
}

package org.kiwiproject.dropwizard.activemq;

import com.google.common.annotations.VisibleForTesting;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.security.KeyAndTrustStoreConfigProvider;

/**
 * This is public mainly to faciliate testing. For example to supply the {@link DropwizardActiveMq#builder()}
 * with a mock or stub object.
 */
@Slf4j
public class ActiveMqHelper {

    private static final int DEFAULT_SEND_TIMEOUT_MILLIS = 5_000;
    private static final int DEFAULT_CONNECT_RESPONSE_TIMEOUT_MILLIS = 5_000;

    /**
     * Create a new {@link PooledConnectionFactory} instance for the given {@link ActiveMqConfig}.
     *
     * @param config the {@link ActiveMqConfig}
     * @return a new PooledConnectionFactory instance
     */
    public PooledConnectionFactory newPooledConnectionFactory(ActiveMqConfig config) {
        return newPooledConnectionFactory(newActiveMQConnectionFactory(config));
    }

    /**
     * Create a new {@link PooledConnectionFactory} instance for the given {@link ActiveMQConnectionFactory}.
     *
     * @param amqFactory the ActiveMQ connection factory
     * @return a new PooledConnectionFactory instance
     */
    public PooledConnectionFactory newPooledConnectionFactory(ActiveMQConnectionFactory amqFactory) {
        var pooledConnectionFactory = new PooledConnectionFactory();
        pooledConnectionFactory.setConnectionFactory(amqFactory);
        return pooledConnectionFactory;
    }

    /**
     * Return an ActiveMQConnectionFactory for non-secure connection configurations, or an
     * ActiveMQSslConnectionFactory for secure connection configurations.
     */
    @VisibleForTesting
    ActiveMQConnectionFactory newActiveMQConnectionFactory(ActiveMqConfig config) {
        ActiveMQConnectionFactory connectionFactory;
        if (config.isUseSecureActiveMQConnections()) {
            connectionFactory = newActiveMQSslConnectionFactory(config);
        } else {
            connectionFactory = new ActiveMQConnectionFactory(config.getBrokerUri());
        }

        LOG.info("Override infinite sendTimeout on ActiveMQConnectionFactory. Set to {}ms",
                DEFAULT_SEND_TIMEOUT_MILLIS);
        connectionFactory.setSendTimeout(DEFAULT_SEND_TIMEOUT_MILLIS);

        LOG.info("Override infinite connectResponseTimeout on ActiveMQConnectionFactory. Set to {}ms",
                DEFAULT_CONNECT_RESPONSE_TIMEOUT_MILLIS);
        connectionFactory.setConnectResponseTimeout(DEFAULT_CONNECT_RESPONSE_TIMEOUT_MILLIS);

        return connectionFactory;
    }

    private ActiveMQSslConnectionFactory newActiveMQSslConnectionFactory(ActiveMqConfig config) {
        var factory = new ActiveMQSslConnectionFactory(config.getBrokerUri());
        var securityConfig = config.getTlsConfiguration();
        configureConnectionFactorySecurity(factory, securityConfig);

        return factory;
    }

    /**
     * Configures the key and trust stores on the {@link ActiveMQSslConnectionFactory}.
     *
     * @implNote The {@link ActiveMQSslConnectionFactory#setKeyStore(String)} and
     * {@link ActiveMQSslConnectionFactory#setTrustStore(String)} methods are defined to throw
     * {@link Exception} even though all they do is set an instance variable and nullify a
     * different one, and therefore cannot actually throw anything at all! This has been the
     * case since SVN revision 1361984 on Mon, July 12, 2012, UTC, and it seems unlikely to
     * change. As a result, we're using Lombok's {@link SneakyThrows} to avoid a try/catch
     * that cannot occur.
     */
    @SneakyThrows
    private static void configureConnectionFactorySecurity(ActiveMQSslConnectionFactory factory,
                                                           KeyAndTrustStoreConfigProvider securityConfig) {

        factory.setKeyStore(securityConfig.getKeyStorePath());
        factory.setKeyStorePassword(securityConfig.getKeyStorePassword());
        factory.setKeyStoreType(securityConfig.getKeyStoreType());
        factory.setTrustStore(securityConfig.getTrustStorePath());
        factory.setTrustStorePassword(securityConfig.getTrustStorePassword());
        factory.setTrustStoreType(securityConfig.getTrustStoreType());
    }
}

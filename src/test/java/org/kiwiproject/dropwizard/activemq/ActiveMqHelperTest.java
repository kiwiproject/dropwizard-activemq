package org.kiwiproject.dropwizard.activemq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory.newTlsContextConfiguration;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;

@DisplayName("ActiveMqHelper")
class ActiveMqHelperTest {

    private ActiveMqHelper helper;
    private ActiveMqConfig config;

    @BeforeEach
    void setUp() {
        helper = new ActiveMqHelper();
        config = new ActiveMqConfig();
    }

    @Nested
    class NewActiveMQConnectionFactory {

        @Test
        void shouldOverrideDefaultInfiniteTimeouts() {
            var factory = helper.newActiveMQConnectionFactory(config);

            assertAll(
                    () -> assertThat(factory.getSendTimeout()).isEqualTo(5_000),
                    () -> assertThat(factory.getConnectResponseTimeout()).isEqualTo(5_000)
            );
        }

        @Test
        void shouldReturnPlainFactory_WhenNotUsingSecureActiveMQConnection() {
            config.setUseSecureActiveMQConnections(false);
            var factory = helper.newActiveMQConnectionFactory(config);
            assertThat(factory).isExactlyInstanceOf(ActiveMQConnectionFactory.class);
        }

        @Test
        void shouldReturnSslFactory_WhenUsingSecureActiveMQConnection() {
            var tlsConfig = newTlsContextConfiguration();
            config.setTlsConfiguration(tlsConfig);

            assertSecureActiveMQConnectionFactory(config);
        }

        private void assertSecureActiveMQConnectionFactory(ActiveMqConfig activeMqConfig) {
            var factory = helper.newActiveMQConnectionFactory(activeMqConfig);
            assertThat(factory).isExactlyInstanceOf(ActiveMQSslConnectionFactory.class);

            var securityConfig = activeMqConfig.getTlsConfiguration();

            var sslFactory = (ActiveMQSslConnectionFactory) factory;
            assertAll(
                    () -> assertThat(sslFactory.getKeyStore()).isEqualTo(securityConfig.getKeyStorePath()),
                    () -> assertThat(sslFactory.getKeyStorePassword()).isEqualTo(securityConfig.getKeyStorePassword()),
                    () -> assertThat(sslFactory.getKeyStoreType()).isEqualTo(securityConfig.getKeyStoreType()),
                    () -> assertThat(sslFactory.getTrustStore()).isEqualTo(securityConfig.getTrustStorePath()),
                    () -> assertThat(sslFactory.getTrustStorePassword()).isEqualTo(securityConfig.getTrustStorePassword()),
                    () -> assertThat(sslFactory.getTrustStoreType()).isEqualTo(securityConfig.getTrustStoreType())
            );
        }
    }
}

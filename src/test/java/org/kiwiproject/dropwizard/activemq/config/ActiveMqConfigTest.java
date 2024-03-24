package org.kiwiproject.dropwizard.activemq.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory.newTlsContextConfiguration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.kiwiproject.config.TlsContextConfiguration;

import java.util.stream.Stream;

@DisplayName("ActiveMqConfig")
class ActiveMqConfigTest {

    private ActiveMqConfig config;

    @BeforeEach
    void setUp() {
        config = new ActiveMqConfig();
    }

    @Nested
    class IsTlsConfigurationValid {

        @ParameterizedTest(name = "[{index}] secureAMQ: {0}, secureRest: {1}, ssl: {2} -> should be {3}")
        @MethodSource("org.kiwiproject.dropwizard.activemq.config.ActiveMqConfigTest#tlsConfigurationArgumentFactory")
        void shouldReturnExpectedResult(boolean useSecureAmqConnections,
                                        boolean useSecureRestConnections,
                                        boolean tlsConfigExists,
                                        boolean expectedResult) {

            config.setUseSecureActiveMQConnections(useSecureAmqConnections);
            config.setUseSecureRestConnections(useSecureRestConnections);
            config.setTlsConfiguration(tlsConfigExists ? newTlsContextConfiguration() : null);

            assertThat(config.isTlsConfigurationValid()).isEqualTo(expectedResult);
        }

        @Test
        void shouldBeFalse_WhenTlsConfiguration_IsNotValid() {
            config.setUseSecureActiveMQConnections(true);
            config.setUseSecureRestConnections(true);
            config.setTlsConfiguration(new TlsContextConfiguration());

            assertThat(config.isTlsConfigurationValid()).isFalse();
        }
    }

    // each Object[] contains: useSecureAmqConnections, useSecureRestConnections, tlsConfigExists, expectedResult
    @SuppressWarnings("unused")
    private static Stream<Object[]> tlsConfigurationArgumentFactory() {
        return Stream.of(
                new Object[] { true, true, true, true },
                new Object[] { true, true, false, false },
                new Object[] { true, false, true, true },
                new Object[] { true, false, false, false },
                new Object[] { false, true, true, true },
                new Object[] { false, true, false, false },
                new Object[] { false, false, true, true },
                new Object[] { false, false, false, true }
        );
    }

    @Nested
    class IsBrokerUriForSslProbablyValid {

        @ParameterizedTest
        @CsvSource(textBlock = """
                false, tcp://host1.prod:61616, true
                false, 'failover:(tcp://host1.prod:61616,tcp://host2.prod:61616)?randomize=false', true
                false, 'ssl://host1.prod:61617', false
                false, 'ssl://host1.prod:61617?verifyHostName=false', false
                false, 'failover:(ssl://host1.prod:61617,ssl://host2.prod:61617)?randomize=false&nested.verifyHostName=false', false

                true, tcp://host1.prod:61616, false
                true, 'failover:(tcp://host1.prod:61616,tcp://host2.prod:61616)?randomize=false', false
                true, 'ssl://host1.prod:61617, true
                true, 'ssl://host1.prod:61617?verifyHostName=false', true
                true, 'failover:(ssl://host1.prod:61617,ssl://host2.prod:61617)?randomize=false&nested.verifyHostName=false', true
                """)
        void shouldReturnExpectedResult(boolean useSecureActiveMQConnections,
                                        String brokerUri,
                                        boolean expectedResult) {

            config.setUseSecureActiveMQConnections(useSecureActiveMQConnections);
        }
    }
}

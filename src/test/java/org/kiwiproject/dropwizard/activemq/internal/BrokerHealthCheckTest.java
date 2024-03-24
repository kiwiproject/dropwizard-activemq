package org.kiwiproject.dropwizard.activemq.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory.uniqueServiceName;
import static org.kiwiproject.test.assertj.dropwizard.metrics.HealthCheckResultAssertions.assertThatHealthCheck;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.codahale.metrics.health.HealthCheck;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.kiwiproject.base.DefaultEnvironment;
import org.kiwiproject.dropwizard.activemq.test.junit.jupiter.EmbeddedActiveMqExtension;
import org.mockito.Mockito;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;
import java.util.concurrent.TimeUnit;

@DisplayName("BrokerHealthCheck")
class BrokerHealthCheckTest {

    @RegisterExtension
    final EmbeddedActiveMqExtension broker = new EmbeddedActiveMqExtension();

    private String serviceName;

    @BeforeEach
    void setUp() {
        serviceName = uniqueServiceName();
    }

    @Test
    void shouldReturnDefaultHealthCheckName() {
        assertThat(BrokerHealthCheck.defaultHealthCheckName()).isEqualTo("ActiveMQ Producer/Consumer");
    }

    @Test
    void shouldCreateHealthCheckNameWithPrefix() {
        assertAll(
                () -> assertThat(BrokerHealthCheck.createHealthCheckNameWithPrefix(null))
                        .isEqualTo("ActiveMQ Producer/Consumer"),

                () -> assertThat(BrokerHealthCheck.createHealthCheckNameWithPrefix(""))
                        .isEqualTo("ActiveMQ Producer/Consumer"),

                () -> assertThat(BrokerHealthCheck.createHealthCheckNameWithPrefix("  \r\n\t  "))
                        .isEqualTo("ActiveMQ Producer/Consumer"),

                () -> assertThat(BrokerHealthCheck.createHealthCheckNameWithPrefix("Internal"))
                        .isEqualTo("Internal ActiveMQ Producer/Consumer")
        );
    }

    @Test
    void shouldBeHealthy_WhenCanProduceAndConsumeMessages() {
        var healthCheck = new BrokerHealthCheck("testBrokerHealthCheck", broker.newConnectionFactory(), serviceName);

        assertThatHealthCheck(healthCheck)
                .isHealthy()
                .hasMessage("This health check can produce to and consume from JMS broker: vm://embedded-broker");
    }

    @Test
    void shouldBeUnhealthy_WhenReturnsUnexpectedText() throws JMSException {
        var factory = broker.newConnectionFactory();
        var healthCheck = spy(new BrokerHealthCheck("testBrokerHealthCheck", factory, serviceName));
        var provider = spy(new BrokerHealthCheck.ConsumerAndProducerProvider(factory, serviceName));
        var consumer = mock(MessageConsumer.class);
        var message = mock(TextMessage.class);

        doReturn(provider).when(healthCheck).newConsumerAndProducerProvider();
        doReturn(consumer).when(provider).getConsumer();

        when(consumer.receive(anyInt())).thenReturn(message);
        when(message.getText()).thenReturn("Unexpected!");

        assertThatHealthCheck(healthCheck)
                .isUnhealthy()
                .hasMessage("This health check CANNOT produce to or consume from JMS broker: vm://embedded-broker");
    }

    @Test
    void shouldBeUnhealthy_WhenTimesOut() throws JMSException {
        var factory = broker.newConnectionFactory();
        var waitTimeMillis = 1L;
        var healthCheck = spy(new BrokerHealthCheck("testBrokerHealthCheck", factory, waitTimeMillis, TimeUnit.MILLISECONDS, serviceName));
        Mockito.doAnswer(invocation -> {
                    // ensure sleep time is much more than health check's timeout
                    var sleepTime = 5 * waitTimeMillis;
                    new DefaultEnvironment().sleepQuietly(sleepTime, TimeUnit.MILLISECONDS);
                    return HealthCheck.Result.unhealthy("should never see this (should time out before getting here!!!)");
                }).when(healthCheck)
                .doProduceConsumeCheck(anyString());

        assertThatHealthCheck(healthCheck)
                .isUnhealthy()
                .hasMessage("There is some problem with the broker (we timed out trying to produce and consume): vm://embedded-broker");
    }

    @Test
    void shouldBeUnhealthy_WhenHealthCheckThrowsException() throws JMSException {
        var factory = broker.newConnectionFactory();
        var healthCheck = spy(new BrokerHealthCheck("testBrokerHealthCheck", factory, serviceName));

        doThrow(new RuntimeException("oops")).when(healthCheck).doProduceConsumeCheck(anyString());

        assertThatHealthCheck(healthCheck)
                .isUnhealthy()
                .hasMessage("There is some problem with the broker (we don't know what happened): vm://embedded-broker");
    }

    @Test
    void shouldGetBrokerUrl_WhenIsActiveMQConnectionFactory() {
        var brokerUrl = "tcp://amq.acme.com:61616";
        var factory = new ActiveMQConnectionFactory(brokerUrl);

        assertThat(BrokerHealthCheck.getBrokerUrl(factory)).contains(brokerUrl);
    }

    @Test
    void shouldGetBrokerUrl_WhenIsPooledFactory_ThatContainsActiveMQConnectionFactory() {
        var brokerUrl = "tcp://amq.acme.com:61616";
        var factory = new ActiveMQConnectionFactory(brokerUrl);
        var pooledFactory = new PooledConnectionFactory();
        pooledFactory.setConnectionFactory(factory);

        assertThat(BrokerHealthCheck.getBrokerUrl(pooledFactory)).contains(brokerUrl);
    }

    @Test
    void shouldGetBrokerUrl_WhenUnknownConnectionFactory() {
        var factory = newNoOpConnectionFactory();

        assertThat(BrokerHealthCheck.getBrokerUrl(factory)).isEmpty();
    }

    @Test
    void shouldGetBrokerUrl_WhenPooledFactory_ContainsUnknownConnectionFactory() {
        var factory = newNoOpConnectionFactory();
        var pooledFactory = new PooledConnectionFactory();
        pooledFactory.setConnectionFactory(factory);

        assertThat(BrokerHealthCheck.getBrokerUrl(pooledFactory)).isEmpty();
    }

    private ConnectionFactory newNoOpConnectionFactory() {
        return new ConnectionFactory() {

            @SuppressWarnings("RedundantThrows")
            @Override
            public Connection createConnection() throws JMSException {
                return null;
            }

            @SuppressWarnings("RedundantThrows")
            @Override
            public Connection createConnection(String userName, String password) throws JMSException {
                return null;
            }

            @Override
            public JMSContext createContext() {
                return null;
            }

            @Override
            public JMSContext createContext(String userName, String password) {
                return null;
            }

            @Override
            public JMSContext createContext(String userName, String password, int sessionMode) {
                return null;
            }

            @Override
            public JMSContext createContext(int sessionMode) {
                return null;
            }
        };
    }
}

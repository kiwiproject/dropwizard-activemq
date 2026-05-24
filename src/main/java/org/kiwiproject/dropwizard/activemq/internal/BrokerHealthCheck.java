package org.kiwiproject.dropwizard.activemq.internal;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.metrics.health.HealthCheckResults.newHealthyResult;
import static org.kiwiproject.metrics.health.HealthCheckResults.newUnhealthyResult;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.jspecify.annotations.Nullable;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.dropwizard.activemq.util.Utils;
import org.kiwiproject.metrics.health.HealthStatus;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Internal health check that verifies broker connectivity by producing a test message to a temporary
 * queue and consuming it back. Reports unhealthy if the round-trip does not complete within the
 * configured timeout.
 */
@Slf4j
public class BrokerHealthCheck extends HealthCheck {

    private static final String DEFAULT_NAME = "ActiveMQ Producer/Consumer";
    private static final long DEFAULT_JMS_CHECK_WAIT_TIME = 5;
    private static final TimeUnit DEFAULT_JMS_CHECK_WAIT_TIME_UNIT = TimeUnit.SECONDS;

    private final String name;
    private final long jmsCheckWaitTime;
    private final TimeUnit jmsCheckWaitTimeUnit;
    private final ConnectionFactory factory;
    private final String serviceName;
    private final long receiveTimeoutMillis;

    public BrokerHealthCheck(String name, 
                             ConnectionFactory factory,
                             String serviceName,
                             ActiveMqConfig configuration) {
        this(name, factory, DEFAULT_JMS_CHECK_WAIT_TIME, DEFAULT_JMS_CHECK_WAIT_TIME_UNIT, serviceName, configuration);
    }

    @VisibleForTesting
    BrokerHealthCheck(String name,
                      ConnectionFactory factory,
                      long jmsCheckWaitTime,
                      TimeUnit jmsCheckWaitTimeUnit,
                      String serviceName,
                      ActiveMqConfig configuration) {

        this.name = requireNotBlank(name);
        this.factory = requireNotNull(factory);
        this.jmsCheckWaitTime = jmsCheckWaitTime;
        this.jmsCheckWaitTimeUnit = requireNotNull(jmsCheckWaitTimeUnit);
        this.serviceName = requireNotBlank(serviceName);

        checkArgumentNotNull(configuration);
        this.receiveTimeoutMillis = configuration.getBrokerHealthCheckConsumerReceiveTimeout().toMilliseconds();
    }

    public static String createHealthCheckNameWithPrefix(String prefix) {
        if (isBlank(prefix)) {
            return defaultHealthCheckName();
        }

        return prefix + " " + defaultHealthCheckName();
    }

    public static String defaultHealthCheckName() {
        return DEFAULT_NAME;
    }

    @Override
    protected Result check() {
        // This health check creates a temporary queue (see ConsumerAndProducerProvider). In ActiveMQ, creating
        // a temporary queue makes a blocking call. When AMQ is down, the call will block until it is alive
        // again. The following code uses a CompletionService to perform the check asynchronously, and waits up
        // to a maximum timeout before reporting as unhealthy.

        var brokerUrl = getBrokerUrl(factory).orElse("[unknown-broker-URL]");
        LOG.trace("Start health check {} for broker: {}", name, brokerUrl);

        var executor = Executors.newSingleThreadExecutor();
        var completionService = new ExecutorCompletionService<Result>(executor);
        try {
            LOG.trace("Submit task to check health in {} for broker: {}", name, brokerUrl);
            completionService.submit(() -> doProduceConsumeCheck(brokerUrl));

            try {
                LOG.trace("Wait up to {} {} for health check result in {} for broker: {}",
                        jmsCheckWaitTime, jmsCheckWaitTimeUnit, name, brokerUrl);
                var resultFuture = completionService.poll(jmsCheckWaitTime, jmsCheckWaitTimeUnit);
                return getResult(resultFuture, brokerUrl);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("InterruptedException waiting for {} health check for broker: {}", name, brokerUrl, e);
                return newUnhealthyResult(HealthStatus.CRITICAL, e,
                        "Interrupted while waiting. Broker: " + brokerUrl);
            } catch (ExecutionException e) {
                LOG.error("Unexpected ExecutionException performing {} health check for broker: {}", name, brokerUrl, e);
                return newUnhealthyResult(HealthStatus.CRITICAL, e,
                        "There is some problem with the broker (we don't know what happened): " + brokerUrl);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @VisibleForTesting
    static Optional<String> getBrokerUrl(ConnectionFactory factory) {
        if (factory instanceof ActiveMQConnectionFactory amqConnectionFactory) {
            return getBrokerUrlFromActiveMQFactory(amqConnectionFactory);
        } else if (factory instanceof PooledConnectionFactory pooledConnectionFactory) {
            return getBrokerUrlFromPooledConnectionFactory(pooledConnectionFactory);
        }

        return Optional.empty();
    }

    private static Optional<String> getBrokerUrlFromPooledConnectionFactory(
            PooledConnectionFactory pooledConnectionFactory) {

        var connectionFactory = pooledConnectionFactory.getConnectionFactory();
        if (connectionFactory instanceof ActiveMQConnectionFactory amqConnectionFactory) {
            return getBrokerUrlFromActiveMQFactory(amqConnectionFactory);
        }

        return Optional.empty();
    }

    private static Optional<String> getBrokerUrlFromActiveMQFactory(ActiveMQConnectionFactory amqConnectionFactory) {
        return Optional.ofNullable(amqConnectionFactory.getBrokerURL());
    }

    @VisibleForTesting
    Result doProduceConsumeCheck(String brokerUrl) throws JMSException {
        try (var provider = newConsumerAndProducerProvider()) {
            var consumer = provider.getConsumer();
            var producer = provider.getProducer();
            var messageText = "test-message-" + System.currentTimeMillis();

            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            producer.send(provider.getSession().createTextMessage(messageText));

            var payload = (TextMessage) consumer.receive(receiveTimeoutMillis);

            if (isNull(payload)) {
                return newUnhealthyResult(
                        "This health check did not receive a message from JMS broker within the timeout: " + brokerUrl);
            }

            if (!messageText.equals(payload.getText())) {
                return newUnhealthyResult(
                        "This health check received an unexpected message from JMS broker: " + brokerUrl);
            }

            return newHealthyResult("This health check can produce to and consume from JMS broker: " + brokerUrl);
        }
    }

    @VisibleForTesting
    ConsumerAndProducerProvider newConsumerAndProducerProvider() throws JMSException {
        return new ConsumerAndProducerProvider(factory, serviceName);
    }

    @Getter
    @VisibleForTesting
    static class ConsumerAndProducerProvider extends SessionProvider {

        private TemporaryQueue queue;
        private MessageConsumer consumer;
        private MessageProducer producer;

        ConsumerAndProducerProvider(ConnectionFactory factory, String serviceName) throws JMSException {
            super(factory, serviceName);

            try {
                queue = session.createTemporaryQueue();
                consumer = session.createConsumer(queue);
                producer = session.createProducer(queue);
            } catch (JMSException e) {
                Utils.safelyClose(consumer, producer, Pair.of(queue, "delete"), session, connection);
                throw e;
            }
        }

        @Override
        public void close() {
            Utils.silentlyRun(consumer::close, producer::close, queue::delete);

            super.close();  // closes session and connection
        }
    }

    private static Result getResult(@Nullable Future<Result> resultFuture, String brokerUrl)
            throws InterruptedException, ExecutionException {

        // CompletionService#poll(long timeout, TimeUnit unit) returns null if the waiting time
        // elapsed before a result is available. So if it is null, return unhealthy result and
        // otherwise get the Result from the Future.

        if (nonNull(resultFuture)) {
            return resultFuture.get();
        }

        return newUnhealthyResult(HealthStatus.CRITICAL,
                "There is some problem with the broker (we timed out trying to produce and consume): %s", brokerUrl);
    }

}

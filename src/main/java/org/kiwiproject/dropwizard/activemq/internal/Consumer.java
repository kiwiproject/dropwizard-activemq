package org.kiwiproject.dropwizard.activemq.internal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.base.KiwiStrings.f;
import static org.kiwiproject.base.UUIDs.randomUUIDString;
import static org.kiwiproject.dropwizard.activemq.ActiveMqMessage.JMS_CORRELATION_ID;
import static org.kiwiproject.dropwizard.activemq.ActiveMqMessage.JMS_CORRELATION_ID_AS_BYTES;
import static org.kiwiproject.dropwizard.activemq.ActiveMqMessage.JMS_DESTINATION;
import static org.kiwiproject.dropwizard.activemq.ActiveMqMessage.JMS_MESSAGE_ID;
import static org.kiwiproject.dropwizard.activemq.ActiveMqMessage.JMS_X_USER_ID;
import static org.kiwiproject.dropwizard.activemq.internal.DestinationExtractor.createElucidationDestination;
import static org.kiwiproject.dropwizard.activemq.internal.TypesDetector.determineContentTypeOf;
import static org.kiwiproject.dropwizard.activemq.internal.TypesDetector.determineMessageTypeFrom;
import static org.kiwiproject.metrics.health.HealthCheckResults.newHealthyResult;
import static org.kiwiproject.metrics.health.HealthCheckResults.newUnhealthyResult;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.annotations.VisibleForTesting;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.jms.pool.PooledMessageConsumer;
import org.kiwiproject.base.DefaultEnvironment;
import org.kiwiproject.base.KiwiEnvironment;
import org.kiwiproject.dropwizard.activemq.ActiveMqConsumer;
import org.kiwiproject.dropwizard.activemq.ActiveMqMessage;
import org.kiwiproject.dropwizard.activemq.ActiveMqConsumer.Result;
import org.kiwiproject.dropwizard.activemq.exception.ActiveMqMessageException;
import org.kiwiproject.dropwizard.activemq.util.Utils;
import org.kiwiproject.elucidation.client.ElucidationClient;
import org.kiwiproject.metrics.health.HealthStatus;

import java.lang.reflect.Field;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.BytesMessage;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;

import io.dropwizard.lifecycle.Managed;

/**
 * This is an internal class that instantiates and manages an {@link ActiveMQMessageConsumer}.
 * <p>
 * It receives messages from the {@link ActiveMQMessageConsumer} and delegates them to
 * an {@link ActiveMqConsumer}.
 */
@Slf4j
public class Consumer implements Managed, Runnable {

    private static final KiwiEnvironment KIWI_ENVIRONMENT = new DefaultEnvironment();
    private static final long ONE_SECOND_IN_MILLIS = TimeUnit.SECONDS.toMillis(1);
    private static final long TEN = 10L;
    private static final long TEN_SECONDS_IN_MILLIS = TimeUnit.SECONDS.toMillis(TEN);

    private final String destination;
    private final ActiveMqConsumer delegateConsumer;
    private final ConnectionFactory factory;
    private final ElucidationClient<String> elucidation;
    private final String serviceName;

    private final String threadName;
    private final Thread thread;

    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final AtomicBoolean consuming = new AtomicBoolean(false);
    private final AtomicReference<Throwable> uncaughtExceptionRef = new AtomicReference<>();

    @VisibleForTesting
    @Getter(AccessLevel.MODULE)
    private final AtomicInteger errors = new AtomicInteger(0);

    public Consumer(ConnectionFactory factory,
                    String destination,
                    ActiveMqConsumer delegateConsumer,
                    ElucidationClient<String> elucidation,
                    String serviceName) {

        this.factory = requireNotNull(factory);
        this.destination = requireNotBlank(destination);
        this.delegateConsumer = requireNotNull(delegateConsumer);
        this.elucidation = requireNotNull(elucidation);
        this.serviceName = requireNotBlank(serviceName);

        threadName = "DelegateConsumer[" + destination + "]";
        thread = new Thread(this, threadName);
        thread.setUncaughtExceptionHandler((theThread, throwable) -> {
            this.uncaughtExceptionRef.set(throwable);
            LOG.error("Received uncaught Throwable in Consumer thread {} which will now terminate and no longer consume messages!",
                    theThread.getName(), throwable);
        });
    }

    /**
     * Is this consumer actively consuming messages?
     */
    boolean isConsuming() {
        return consuming.get();
    }

    @Override
    public void run() {
        while (!stopping.get()) {

            try (var provider = newConsumerProvider()) {

                if (provider.isActiveMQMessageConsumer()) {
                    LOG.trace("Set consuming to true for ActiveMQMessageConsumer");
                    consuming.set(true);
                    loop(provider.getActiveMQMessageConsumer());
                } else if (provider.isPooledMessageConsumer()) {
                    LOG.trace("Set consuming to true for PooledMessageConsumer");
                    consuming.set(true);
                    loop(provider.extractDelegate());
                } else {
                    LOG.error("JMS consumer was not an ActiveMQMessageConsumer and is not currently supported!");
                }

            } catch (Exception e) {
                var errorCount = errors.incrementAndGet();
                LOG.warn("Failure ${} - attempting to recover...", errorCount);
                LOG.error("Consumer failure exception:", e);
                KIWI_ENVIRONMENT.sleepQuietly(ONE_SECOND_IN_MILLIS);

            } finally {
                LOG.info("Setting consuming to false");
                consuming.set(false);
            }
        }
    }

    private ConsumerProvider newConsumerProvider() throws JMSException {
        return new ConsumerProvider(factory, destination, serviceName);
    }

    private void loop(ActiveMQMessageConsumer consumer) throws JMSException {
        while (!stopping.get()) {
            LOG.trace("Wait to receive next message...");

            try {
                var message = consumer.receive(400);  // TODO Make configurable and/or extract constant?
                LOG.trace("Received a message or 'receive' timed out; reset errors to zero");
                errors.set(0);

                if (nonNull(message)) {
                    delegate(createActiveMqMessageFrom(message));
                }

            } catch (UnknownMessageTypeException e) {
                LOG.error("Unknown message type", e);
                consumer.rollback();
            } finally {
                Correlation.CORRELATION_ID.remove();
            }
        }

        LOG.trace("Exiting loop...");
    }

    private void delegate(ActiveMqMessage activeMqMessage) {
        try {
            // Due to the ensureCorrelationIdExists, the orElseGet path should never happen, but just in case...
            Correlation.CORRELATION_ID.set(activeMqMessage.getJMSCorrelationID().orElseGet(() -> randomUUIDString()));

            var consumptionResult = delegateConsumer.consume(activeMqMessage);
            Optional<String> activeMqMessageType = activeMqMessage.getMessageType();

            var consumerName = activeMqMessage.getJMSDestination()
                    .map(DestinationExtractor::simplifyDestination)
                    .orElse("UNKNOWN DESTINATION");

            if (consumptionResult == Result.CONSUMED && activeMqMessageType.isPresent()) {
                var elucidationDestination = createElucidationDestination(consumerName, activeMqMessageType.get());
                elucidation.recordNewEvent(elucidationDestination).whenComplete(ElucidationLogger::logResult);
            } else {
                LOG.trace("Elucidation event NOT created. consumerName: {}, activeMqMessageType: {}, consumptionResult: {}",
                        consumerName, activeMqMessageType, consumptionResult);
            }

        } catch (ActiveMqMessageException e) {
            delegateConsumer.handleException(activeMqMessage, e);
        } finally {
            Correlation.CORRELATION_ID.remove();
        }
    }

    private static ActiveMqMessage createActiveMqMessageFrom(Message message) throws JMSException {
        String body;
        ActiveMqMessage.ContentType contentType;
        String messageType;

        if (message instanceof TextMessage textMessage) {
            body = textMessage.getText();
            contentType = determineContentTypeOf(body);
            messageType = determineMessageTypeFrom(body, contentType);
        } else if (message instanceof BytesMessage bytesMessage) {
            body = getByteMessageBodyFrom(bytesMessage);
            contentType = ActiveMqMessage.ContentType.BYTES;
            messageType = ActiveMqMessage.ContentType.BYTES.convertToMessageType();
        } else {
            throw new UnknownMessageTypeException(message.getClass());
        }

        Map<String, Object> messageProperties = getPropertiesFrom(message);

        LOG.trace("Creating ActiveMqMessage with contentType: [{}], messageType: [{}], messageProperties: {}",
                contentType, messageType, messageProperties);

        return ActiveMqMessage.builder()
                .body(body)
                .contentType(contentType)
                .messageType(messageType)
                .properties(messageProperties)
                .build();
    }

    private static String getByteMessageBodyFrom(BytesMessage bytesMessage) throws JMSException {
        var bytes = new byte[(int) bytesMessage.getBodyLength()];
        bytesMessage.readBytes(bytes);

        return Base64.getEncoder().encodeToString(bytes);
    }

    private static Map<String, Object> getPropertiesFrom(Message message) throws JMSException {
        Map<String, Object> properties = new HashMap<>();

        // Most of the standard Message headers are also included in ActiveMQ's properties'
        var enumeration = ((ActiveMQMessage) message).getAllPropertyNames();
        while (enumeration.hasMoreElements()) {
            var property = (String) enumeration.nextElement();

            if (JMS_CORRELATION_ID.equals(property)) {
                ensureCorrelationIdExists(message, properties);
            } else {
                var propertyValue = message.getObjectProperty(property);
                LOG.trace("ActiveMQ property found: {} -> {}", property, propertyValue);
                properties.put(property, propertyValue);
            }
        }

        // Add the remaining standard Message headers that *aren't* listed as properties in ActiveMQ
        var jmsDestination = message.getJMSDestination();
        LOG.trace("ActiveMQ property found: JMS_DESTINATION -> {}", jmsDestination);
        properties.put(JMS_DESTINATION, jmsDestination);

        var jmsMessageId = message.getJMSMessageID();
        LOG.trace("ActiveMQ property found: JMS_MESSAGE_ID -> {}", jmsMessageId);
        properties.put(JMS_MESSAGE_ID, jmsMessageId);

        // Also add ActiveMQ's JMSXUserId, which inexplicably is *not* listed in ActiveMQ's getAllPropertyNames()
        var jmsXUserId = message.getObjectProperty(JMS_X_USER_ID);
        LOG.trace("ActiveMQ JMS property: JMS_X_USER_ID -> {}", jmsXUserId);
        properties.put(JMS_X_USER_ID, jmsXUserId);

        return properties;
    }

    private static void ensureCorrelationIdExists(Message message, Map<String, Object> properties) throws JMSException {
        var correlationId = message.getJMSCorrelationID();
        var correlationIdAsBytes = message.getJMSCorrelationIDAsBytes();

        if (isBlank(correlationId)) {
            correlationId = randomUUIDString();
            correlationIdAsBytes = correlationId.getBytes(UTF_8);
        }

        properties.put(JMS_CORRELATION_ID, correlationId);
        properties.put(JMS_CORRELATION_ID_AS_BYTES, correlationIdAsBytes);
    }

    @Override
    public void start() throws Exception {
        LOG.info("Starting thread '{}'", threadName);
        thread.start();
    }

    @Override
    public void stop() throws Exception {
        LOG.info("Waiting until thread '{}' is stopped... (is currently alive? {})",
                threadName, thread.isAlive());

        if (thread.isAlive()) {
            stopping.set(true);

            LOG.trace("Wait up to {} seconds for thread '{}'' to die", TEN, threadName);
            thread.join(TEN_SECONDS_IN_MILLIS);

            // TODO Maybe split into INFO and WARN level based on whether is still alive or not (WARN if still alive)
            LOG.info("Thread '{}' is stopped or we timed out waiting for it to die (is still alive? {})",
                    threadName, thread.isAlive());
        }
    }

    public HealthCheck getHealthCheck() {
        return new HealthCheck() {

            @Override
            protected Result check() {
                if (isConsuming()) {
                    return newHealthyResult("%s is consuming messages from %s", threadName, destination);
                }

                return unhealthyConsumerResult();
            }

            private Result unhealthyConsumerResult() {
                var throwable = uncaughtExceptionRef.get();

                if (isNull(throwable)) {
                    return newUnhealthyResult("%s is NOT consuming messages from %s!",
                            threadName, destination);
                }

                return newUnhealthyResult(
                    HealthStatus.CRITICAL,
                    throwable,
                    "%s is NOT consuming messages from %s due to uncaught exception!",
                    threadName, destination
                );
            }
        };
    }

    @VisibleForTesting
    static class ConsumerProvider extends SessionProvider {

        private static final Field DELEGATE;

        static {
            try {
                DELEGATE = PooledMessageConsumer.class.getDeclaredField("delegate");
                DELEGATE.setAccessible(true);
            } catch (Exception e) {
                throw new ConsumerProviderException(
                    "Unable to get 'delegate' field or make it accessible (maybe ActiveMQ is now using the JPMS?)",
                    e);
            }
        }

        @Getter
        private MessageConsumer consumer;

        ConsumerProvider(ConnectionFactory factory, String uri, String serviceName) throws JMSException {
            super(factory, serviceName);

            try {
                consumer = session.createConsumer(newDestination(uri, session, false));
            } catch (JMSException e) {
                LOG.warn("Caught JMSException: errorCode={}, message={} (enable DEBUG for stack traces)",
                        e.getErrorCode(), e.getMessage());
                LOG.debug("JMSException:", e);
                throw e;
            }
        }

        @Override
        public void close() {
            Utils.silentlyRun(consumer::close);

            super.close();
        }

        boolean isActiveMQMessageConsumer() {
            return consumer instanceof ActiveMQMessageConsumer;
        }

        boolean isPooledMessageConsumer() {
            return consumer instanceof PooledMessageConsumer;
        }

        ActiveMQMessageConsumer getActiveMQMessageConsumer() {
            return (ActiveMQMessageConsumer) getConsumer();
        }

        ActiveMQMessageConsumer extractDelegate() {
            try {
                return (ActiveMQMessageConsumer) DELEGATE.get(consumer);
            } catch (IllegalAccessException e) {
                throw new ConsumerProviderException("Unable to access delegate on consumer", e);
            }
        }
    }

    private static class UnknownMessageTypeException extends RuntimeException {
        UnknownMessageTypeException(Class<?> type) {
            super(f("Type '{}' is not handled!", type.getSimpleName()));
        }
    }

    private static class ConsumerProviderException extends RuntimeException {
        ConsumerProviderException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

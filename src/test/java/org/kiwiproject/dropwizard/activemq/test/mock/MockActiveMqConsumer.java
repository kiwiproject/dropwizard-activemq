package org.kiwiproject.dropwizard.activemq.test.mock;

import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotBlank;
import static org.kiwiproject.base.KiwiStrings.f;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.extern.slf4j.Slf4j;
import org.kiwiproject.dropwizard.activemq.ActiveMqConsumer;
import org.kiwiproject.dropwizard.activemq.ActiveMqMessage;
import org.kiwiproject.dropwizard.activemq.exception.ActiveMqMessageException;
import org.kiwiproject.dropwizard.activemq.util.UncheckedJMSException;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class MockActiveMqConsumer implements ActiveMqConsumer {

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private final Multimap<String, String> consuming = ArrayListMultimap.create();
        private final Multimap<String, String> ignoring = ArrayListMultimap.create();
        private Error error;
        private boolean validateBodyIsPresentOrThrowException;
        private boolean validateMessageTypeIsPresentOrThrowException;

        public Builder consumeMessagesOfType(String destination, String... types) {
            return consumeMessagesOfType(destination, List.of(types));
        }

        public Builder consumeMessagesOfType(String destination, Collection<String> types) {
            populateMapWith(consuming, destination, types);
            return this;
        }

        public Builder ignoringMessagesOfType(String destination, String... types) {
            return ignoringMessagesOfType(destination, List.of(types));
        }

        public Builder ignoringMessagesOfType(String destination, Collection<String> types) {
            populateMapWith(ignoring, destination, types);
            return this;
        }

        private void populateMapWith(Multimap<String, String> map, String destination, Collection<String> types) {
            types.forEach(type -> map.put(destination, type));
        }

        public Builder validateBodyIsPresentOrThrowException() {
            this.validateBodyIsPresentOrThrowException = true;
            return this;
        }

        public Builder validateMessageTypeIsPresentOrThrowException() {
            this.validateMessageTypeIsPresentOrThrowException = true;
            return this;
        }

        /**
         * Allows simulation of an uncaught exception. If set, the consumer will stop consuming.
         */
        public Builder throwError(Error error) {
            this.error = error;
            return this;
        }

        public MockActiveMqConsumer buildConsumer() {
            return new MockActiveMqConsumer(
                    consuming,
                    ignoring,
                    error,
                    validateBodyIsPresentOrThrowException,
                    validateMessageTypeIsPresentOrThrowException
            );
        }
    }

    private final Multimap<String, ActiveMqMessage> consumedMessages = ArrayListMultimap.create();
    private final Multimap<String, ActiveMqMessage> ignoredMessages = ArrayListMultimap.create();

    private final Multimap<String, String> consuming = ArrayListMultimap.create();
    private final Multimap<String, String> ignoring = ArrayListMultimap.create();
    private Error error;
    private boolean validateBodyIsPresentOrThrowException;
    private boolean validateMessageTypeIsPresentOrThrowException;

    private final AtomicLong receivedCount = new AtomicLong();

    private MockActiveMqConsumer(Multimap<String, String> consuming,
                                 Multimap<String, String> ignoring,
                                 Error error,
                                 boolean validateBodyIsPresentOrThrowException,
                                 boolean validateMessageTypeIsPresentOrThrowException) {

        this.consuming.putAll(consuming);
        this.ignoring.putAll(ignoring);
        this.error = error;
        this.validateBodyIsPresentOrThrowException = validateBodyIsPresentOrThrowException;
        this.validateMessageTypeIsPresentOrThrowException = validateMessageTypeIsPresentOrThrowException;
    }

    @Override
    public Result consume(ActiveMqMessage message) {
        receivedCount.incrementAndGet();

        if (nonNull(error)) {
            throw error;
        }

        if (validateBodyIsPresentOrThrowException) {
            var body = requireValidBody(message);
            LOG.trace("body of message: {}", body);
        }

        String messageType;
        if (validateMessageTypeIsPresentOrThrowException) {
            LOG.trace("validating message type");
            messageType = requireValidMessageType(message);
        } else {
            messageType = message.getMessageType().orElse("");
        }

        LOG.trace("message type: [{}]", messageType);

        String destination = getDestinationNameThatConsumed(message);

        if (!consuming.containsKey(destination) && !ignoring.containsKey(destination)) {
            LOG.warn("Encountered destination [{}] in message that was not configured; ignoring message", destination);
            ignoredMessages.put(destination, message);
            return Result.IGNORED;
        }

        if (consuming.get(destination).contains(messageType)) {
            LOG.trace("Encountered type [{}] at [{}] that was set to consume; consuming message", messageType, destination);
            consumedMessages.put(destination, message);
            return Result.CONSUMED;
        }

        if (ignoring.get(destination).contains(messageType)) {
            LOG.trace("Encountered type [{}] at [{}] that was set to ignore; ignoring message", messageType, destination);
            ignoredMessages.put(destination, message);
            return Result.IGNORED;
        }

        LOG.warn("Encountered type [{}] at [{}] that was not configured; ignoring message",
                isNotBlank(messageType) ? messageType : "[blank string]", destination);
        ignoredMessages.put(destination, message);
        return Result.IGNORED;
    }

    private static String getDestinationNameThatConsumed(ActiveMqMessage message) {
        if (message.wasConsumedFromAQueue()) {
            return message.getJMSDestinationAsQueue()
                    .map(MockActiveMqConsumer::getQueueName)
                    .orElseThrow(() -> new IllegalStateException(dueToMissingDestinationOfType("queue")));
        }

        if (message.wasConsumedFromATopic()) {
            return message.getJMSDestinationAsTopic()
                    .map(MockActiveMqConsumer::getTopicName)
                    .orElseThrow(() -> new IllegalStateException(dueToMissingDestinationOfType("topic")));
        }

        throw new IllegalStateException("Somehow you have found a message that was produced without a queue or a topic!");
    }

    private static String getQueueName(Queue queue) {
        try {
            return queue.getQueueName();
        } catch (JMSException e) {
            throw new UncheckedJMSException(e);
        }
    }

    private static String getTopicName(Topic topic) {
        try {
            return topic.getTopicName();
        } catch (JMSException e) {
            throw new UncheckedJMSException(e);
        }
    }

    private static String dueToMissingDestinationOfType(String type) {
        return f("Somehow the message thinks it was consumed by a {}, but the {} doesn't have a name", type, type);
    }

    @Override
    public void handleException(ActiveMqMessage message, ActiveMqMessageException e) {
        LOG.warn("message: {}", message, e);
        throw e;
    }

    public void clear() {
        clearConsumedMessages();
        clearIgnoredMessages();
    }

    public void clearConsumedMessages() {
        consumedMessages.clear();
    }

    public void clearIgnoredMessages() {
        ignoredMessages.clear();
    }

    public List<ActiveMqMessage> consumedHistory() {
        return history(consumedMessages);
    }

    public List<ActiveMqMessage> consumedHistory(String destination) {
        return history(destination, consumedMessages);
    }

    public List<ActiveMqMessage> ignoredHistory() {
        return history(ignoredMessages);
    }

    public List<ActiveMqMessage> ignoredHistory(String destination) {
        return history(destination, ignoredMessages);
    }

    private List<ActiveMqMessage> history(Multimap<String, ActiveMqMessage> messages) {
        return List.copyOf(messages.values());
    }

    private List<ActiveMqMessage> history(String destination, Multimap<String, ActiveMqMessage> messages) {
        checkArgumentNotBlank(destination);
        return List.copyOf(messages.get(destination));
    }

    public long getReceivedCount() {
        return receivedCount.get();
    }
}

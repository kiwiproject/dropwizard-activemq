package org.kiwiproject.dropwizard.activemq;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;
import org.apache.activemq.command.ActiveMQDestination;

import java.util.Map;
import java.util.Optional;

import javax.jms.Queue;
import javax.jms.Topic;

@Value
@Builder
@ToString(onlyExplicitlyIncluded = true)
public class ActiveMqMessage {

    public enum ContentType {
        JSON("JSON_MESSAGE"),
        TEXT("TEXT_MESSAGE"),
        BYTES("BYTES_MESSAGE");

        @Getter
        @Accessors(fluent = true)
        private final String convertToMessageType;

        ContentType(String convertToMessageType) {
            this.convertToMessageType = convertToMessageType;
        }
    }

    // Message header keys that are explicitly listed in ActiveMQ as property keys
    public static final String JMS_CORRELATION_ID = "JMSCorrelationID";
    public static final String JMS_DELIVERY_MODE = "JMSDeliveryMode";
    public static final String JMS_EXPIRATION = "JMSExpiration";
    public static final String JMS_PRIORITY = "JMSPriority";
    public static final String JMS_REDELIVERED = "JMSRedelivered";
    public static final String JMS_REPLY_TO = "JMSReplyTo";
    public static final String JMS_TIMESTAMP = "JMSTimestamp";
    public static final String JMS_TYPE = "JMSType";

    // Message header keys that are NOT explicitly listed in ActiveMQ as property keys
    public static final String JMS_CORRELATION_ID_AS_BYTES = "JMSCorrelationIDAsBytes";
    public static final String JMS_DESTINATION = "JMSDestination";
    public static final String JMS_MESSAGE_ID = "JMSMessageID";

    // ActiveMQ-specific Message property keys
    public static final String JMS_X_DELIVERY_COUNT = "JMSXDeliveryCount";
    public static final String JMS_X_GROUP_ID = "JMSXGroupID";
    public static final String JMS_X_GROUP_SEQ = "JMSXGroupSeq";
    public static final String JMS_X_USER_ID = "JMSXUserID";

    // Library-specific property keys
    public static final String KIWI_AMQ_CONTENT_TYPE_KEY = "Kiwi-AMQ-Content-Type";

    /**
     * The body of the message.
     */
    @Getter(AccessLevel.NONE)
    String body;

    /**
     * The content type of the message, i.e., JSON, TEXT, BYTES.
     */
    @Getter(AccessLevel.NONE)
    @ToString.Include
    ContentType contentType;

    /**
     * The type of message. If {@link #getContentType()} is "JSON", then this will be the result of
     * {@link org.kiwiproject.dropwizard.activemq.util.MessageTypeParser#findType(String)}.
     * <p>
     * If it is a different but recognized content type, it will be the result of
     * {@link ContentType#convertToMessageType()}
     */
    @Getter(AccessLevel.NONE)
    @ToString.Include
    String messageType;

    @ToString.Include
    Map<String, Object> properties;

    public Optional<String> getBody() {
        return Optional.ofNullable(body);
    }

    public Optional<ContentType> getContentType() {
        return Optional.ofNullable(contentType);
    }

    public Optional<String> getMessageType() {
        return Optional.ofNullable(messageType);
    }

    public Optional<String> getJMSCorrelationID() {
        return Optional.ofNullable((String) properties.get(JMS_CORRELATION_ID));
    }

    public Optional<byte[]> getJMSCorrelationIDAsBytes() {
        return Optional.ofNullable((byte[]) properties.get(JMS_CORRELATION_ID_AS_BYTES));
    }

    public Optional<String> getJMSDeliveryMode() {
        return Optional.ofNullable((String) properties.get(JMS_DELIVERY_MODE));
    }

    public Optional<Queue> getJMSDestinationAsQueue() {
        return getJMSDestination()
                .filter(ActiveMQDestination::isQueue)
                .map(Queue.class::cast);
    }

    public Optional<Topic> getJMSDestinationAsTopic() {
        return getJMSDestination()
                .filter(ActiveMQDestination::isTopic)
                .map(Topic.class::cast);
    }

    public Optional<ActiveMQDestination> getJMSDestination() {
        return Optional.ofNullable((ActiveMQDestination) properties.get(JMS_DESTINATION));
    }

    public Optional<Long> getJMSExpiration() {
        return Optional.ofNullable((Long) properties.get(JMS_EXPIRATION));
    }

    public Optional<String> getJMSMessageID() {
        return Optional.ofNullable((String) properties.get(JMS_MESSAGE_ID));
    }

    public Optional<Integer> getJMSPriority() {
        return Optional.ofNullable((Integer) properties.get(JMS_PRIORITY));
    }

    public Optional<Boolean> getJMSRedelivered() {
        return Optional.ofNullable((Boolean) properties.get(JMS_REDELIVERED));
    }

    public Optional<ActiveMQDestination> getJMSReplyTo() {
        return Optional.ofNullable((ActiveMQDestination) properties.get(JMS_REPLY_TO));
    }

    public Optional<Long> getJMSTimestamp() {
        return Optional.ofNullable((Long) properties.get(JMS_TIMESTAMP));
    }

    public Optional<String> getJMSType() {
        return Optional.ofNullable((String) properties.get(JMS_TYPE));
    }

    public Optional<Integer> getJMSXDeliveryCount() {
        return Optional.ofNullable((Integer) properties.get(JMS_X_DELIVERY_COUNT));
    }

    public Optional<String> getJMSXGroupId() {
        return Optional.ofNullable((String) properties.get(JMS_X_GROUP_ID));
    }

    public Optional<Integer> getJMSXGroupSeq() {
        return Optional.ofNullable((Integer) properties.get(JMS_X_GROUP_SEQ));
    }

    public Optional<String> getJMSXUserId() {
        return Optional.ofNullable((String) properties.get(JMS_X_USER_ID));
    }

    public boolean wasConsumedFromAQueue() {
        return getJMSDestination().map(ActiveMQDestination::isQueue).orElse(false);
    }

    public boolean wasConsumedFromATopic() {
        return getJMSDestination().map(ActiveMQDestination::isTopic).orElse(false);
    }
}

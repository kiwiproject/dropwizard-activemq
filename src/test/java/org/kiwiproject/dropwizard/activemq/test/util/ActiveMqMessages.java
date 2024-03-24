package org.kiwiproject.dropwizard.activemq.test.util;

import static org.kiwiproject.test.constants.KiwiTestConstants.JSON_HELPER;

import lombok.experimental.UtilityClass;
import org.kiwiproject.dropwizard.activemq.ActiveMqMessage;
import org.kiwiproject.dropwizard.activemq.ActiveMqMessage.ContentType;
import org.kiwiproject.dropwizard.activemq.util.MessageTypeParser;
import org.kiwiproject.xml.KiwiXml;

import java.util.Base64;
import java.util.Map;

/**
 * Test utilities for creating {@link ActiveMqMessage} instances.
 */
@UtilityClass
public class ActiveMqMessages {

    private static final MessageTypeParser MESSAGE_TYPE_PARSER = new MessageTypeParser(JSON_HELPER);

    // BYTES factory methods

    public static ActiveMqMessage newBytesActiveMqMessage(byte[] bytes) {
        return newBytesActiveMqMessage(bytes, Map.of());
    }

    public static ActiveMqMessage newBytesActiveMqMessage(byte[] bytes,
                                                          Map<String, Object> properties) {
        return newActiveMqMessage(
            Base64.getEncoder().encodeToString(bytes),
            ContentType.BYTES,
            ContentType.BYTES.convertToMessageType(),
            properties);
    }

    // JSON factory methods

    public static ActiveMqMessage newJsonActiveMqMessage(Object object) {
        return newJsonActiveMqMessage(JSON_HELPER.toJson(object));
    }

    public static ActiveMqMessage newJsonActiveMqMessage(String body) {
        var messageType = MESSAGE_TYPE_PARSER.findType(body);

        return newActiveMqMessage(body, ContentType.JSON, messageType, Map.of());
    }

    public static ActiveMqMessage newJsonActiveMqMessage(Object object, String messageType) {
        return newJsonActiveMqMessage(JSON_HELPER.toJson(object), messageType);
    }

    public static ActiveMqMessage newJsonActiveMqMessage(String body, String messageType) {
        return newActiveMqMessage(body, ContentType.JSON, messageType, Map.of());
    }

    public static ActiveMqMessage newJsonActiveMqMessage(Object object,
                                                         Map<String, Object> properties) {
        return newJsonActiveMqMessage(JSON_HELPER.toJson(object), properties);
    }

    public static ActiveMqMessage newJsonActiveMqMessage(String body,
                                                         Map<String, Object> properties) {
        var messageType = MESSAGE_TYPE_PARSER.findType(body);

        return newActiveMqMessage(body, ContentType.JSON, messageType, properties);
    }

    public static ActiveMqMessage newJsonActiveMqMessage(Object object,
                                                         String messageType,
                                                         Map<String, Object> properties) {

        return newJsonActiveMqMessage(JSON_HELPER.toJson(object), messageType, properties);
    }

    public static ActiveMqMessage newJsonActiveMqMessage(String body,
                                                         String messageType,
                                                         Map<String, Object> properties) {
        return newActiveMqMessage(body, ContentType.JSON, messageType, properties);
    }

    // TEXT factory methods

    public static ActiveMqMessage newXmlTextActiveMqMessage(Object object) {
        return newTextActiveMqMessage(KiwiXml.toXml(object));
    }

    public static ActiveMqMessage newTextActiveMqMessage(String body) {
        return newActiveMqMessage(body,
                ContentType.TEXT,
                ContentType.TEXT.convertToMessageType(),
                Map.of());
    }

    public static ActiveMqMessage newXmlTextActiveMqMessage(Object object,
                                                            Map<String, Object> properties) {
        return newTextActiveMqMessage(KiwiXml.toXml(object), properties);
    }

    public static ActiveMqMessage newTextActiveMqMessage(String body, Map<String, Object> properties) {
        return newActiveMqMessage(body,
                ContentType.TEXT,
                ContentType.TEXT.convertToMessageType(),
                properties);
    }

    public static ActiveMqMessage newActiveMqMessage(String body,
                                                     ContentType contentType,
                                                     String messageType,
                                                     Map<String, Object> properties) {

        return ActiveMqMessage.builder()
                .body(body)
                .contentType(contentType)
                .messageType(messageType)
                .properties(properties)
                .build();
    }
}

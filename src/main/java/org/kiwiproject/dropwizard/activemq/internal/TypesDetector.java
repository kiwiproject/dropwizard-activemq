package org.kiwiproject.dropwizard.activemq.internal;

import static org.kiwiproject.dropwizard.activemq.ActiveMqMessage.ContentType.JSON;
import static org.kiwiproject.dropwizard.activemq.ActiveMqMessage.ContentType.TEXT;
import static org.kiwiproject.dropwizard.activemq.util.MessageTypeParser.UNKNOWN_MESSAGE_TYPE;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import org.kiwiproject.dropwizard.activemq.ActiveMqMessage;
import org.kiwiproject.dropwizard.activemq.ActiveMqMessage.ContentType;
import org.kiwiproject.dropwizard.activemq.util.MessageTypeParser;
import org.kiwiproject.json.JsonHelper;

@Slf4j
@UtilityClass
class TypesDetector {

    private static final JsonHelper JSON_HELPER = JsonHelper.newDropwizardJsonHelper();
    private static final MessageTypeParser MESSAGE_TYPE_PARSER = new MessageTypeParser(JSON_HELPER);

    static ActiveMqMessage.ContentType determineContentTypeOf(String payload) {
        return JSON_HELPER.isJson(payload) ?  JSON :  TEXT;
    }

    static String determineMessageTypeFrom(String payload) {
        var contentType = determineContentTypeOf(payload);
        return determineMessageTypeFrom(payload, contentType);
    }

    static String determineMessageTypeFrom(String payload, ContentType contentType) {
        if (JSON == contentType) {
            var messageType = MESSAGE_TYPE_PARSER.findTypeSafe(payload).orElse(UNKNOWN_MESSAGE_TYPE);
            LOG.trace("JSON detected --> messageType identified as [{}]", messageType);
            return messageType;
        }

        var messageType = contentType.convertToMessageType();
        LOG.trace("{} detected -> message identified as [{}]", messageType);
        return messageType;
    }}

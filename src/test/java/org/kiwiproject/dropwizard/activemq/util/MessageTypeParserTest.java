package org.kiwiproject.dropwizard.activemq.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.kiwiproject.test.constants.KiwiTestConstants.JSON_HELPER;

import com.google.common.base.VerifyException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.kiwiproject.json.RuntimeJsonException;

import java.util.List;
import java.util.Set;

@DisplayName("MessageTypeParser")
class MessageTypeParserTest {

    private MessageTypeParser typeParser;

    @BeforeEach
    void setUp() {
        typeParser = new MessageTypeParser(JSON_HELPER);
    }

    @Nested
    class ShouldReturnUnknownType {

        @Test
        void whenNoMessageType() {
            var json = """
                    {
                        "messageTypeMissing": "STATUS_CHANGE"
                    }
                    """;

            assertAll(
                () -> assertThat(typeParser.findAllTypes(json)).isEmpty(),
                () -> assertThat(typeParser.isEchoedMessage(json)).isFalse(),
                () -> assertUnknownMessageType(json)
            );
        }

        @Test
        void whenNullMessageType() {
            var json = """
                    {
                        "messageType": null
                    }
                    """;

            assertAll(
                () -> assertThat(typeParser.findAllTypes(json)).isEmpty(),
                () -> assertThat(typeParser.isEchoedMessage(json)).isFalse(),
                () -> assertUnknownMessageType(json)
            );
        }

        @ParameterizedTest
        @ValueSource(strings = { "", " ", "   "})
        void whenBlankMessageType(String value) {
            var json = """
                    {
                        "messageType": "%s"
                    }
                    """.formatted(value);

            assertAll(
                    () -> assertThat(typeParser.findAllTypes(json)).isEmpty(),
                    () -> assertThat(typeParser.isEchoedMessage(json)).isFalse(),
                    () -> assertUnknownMessageType(json));
        }

        @Test
        void whenNullMessageType_InEchoedMessage() {
            var json = """
                {
                    "messageType": "ECHO_MESSAGE",
                    "echoedMessage": {
                        "messageType": null
                    }
                }
                """;

            assertAll(
                () -> assertThat(typeParser.findAllTypes(json)).containsOnly("ECHO_MESSAGE"),
                () -> assertThat(typeParser.isEchoedMessage(json)).isTrue(),
                () -> assertUnknownMessageType(json)
            );
        }

        @ParameterizedTest
        @ValueSource(strings = { "", " ", "   "})
        void whenBlankMessageType_InEchoedMessage(String value) {
            var json = """
                {
                    "messageType": "ECHO_MESSAGE",
                    "echoedMessage": {
                        "messageType": "%s"
                    }
                }
                """.formatted(value);

            assertAll(
                () -> assertThat(typeParser.findAllTypes(json)).containsOnly("ECHO_MESSAGE"),
                () -> assertThat(typeParser.isEchoedMessage(json)).isTrue(),
                () -> assertUnknownMessageType(json)
            );
        }

        private void assertUnknownMessageType(String json) {
            assertThat(typeParser.findType(json)).isEqualTo("UNKNOWN");
            assertThat(typeParser.findTypeSafe(json)).contains("UNKNOWN");
        }
    }

    @Nested
    class WhenMultipleMessageTypes {

        @ParameterizedTest
        @MethodSource("org.kiwiproject.dropwizard.activemq.util.MessageTypeParserTest#messagesWithMultipleMessageTypes")
        void shouldAssignExpectedMessageType(String description, String json) {
            var statusChangeType = "STATUS_CHANGE";

            assertAll(
                () -> assertThat(typeParser.findAllTypes(json)).describedAs(description).containsOnly(statusChangeType),
                () -> assertThat(typeParser.findType(json)).describedAs(description).isEqualTo(statusChangeType),
                () -> assertThat(typeParser.findTypeSafe(json)).describedAs(description).contains(statusChangeType),
                () -> assertThat(typeParser.isEchoedMessage(json)).describedAs(description).isFalse()
            );
        }
    }

    static List<Arguments> messagesWithMultipleMessageTypes() {
        return List.of(
            Arguments.of("should ignore duplicates", """
                    {
                        "messageType": "STATUS_CHANGE",
                        "messageType": "STATUS_CHANGE"
                    }
                    """),
            Arguments.of("should ignore capitalization differences", """
                    {
                        "messageType": "Status_Change",
                        "messageType": "STATUS_CHANGE"
                    }
                    """),
            Arguments.of("should choose last one", """
                    {
                        "messageType": "ORDER_CREATED",
                        "messageType": "STATUS_CHANGE"
                    }
                    """)
        );
    }

    @Nested
    class WhenValidMessages {

        @Test
        void shouldFindMessageTypeOfNormalMessage() {
            var json = """
                    {
                        "messageType": "ORDER_CREATED",
                        "metaData": {}
                    }
                    """;

            var orderCreatedType = "ORDER_CREATED";

            assertAll(
                () -> assertThat(typeParser.findAllTypes(json)).containsOnly(orderCreatedType),
                () -> assertThat(typeParser.findType(json)).isEqualTo(orderCreatedType),
                () -> assertThat(typeParser.findTypeSafe(json)).contains(orderCreatedType),
                () -> assertThat(typeParser.isEchoedMessage(json)).isFalse()
            );
        }

        @Test
        void shouldFindMessageTypeOfEchoedMessage() {
            var json = """
                    {
                        "messageType": "ECHO_MESSAGE",
                        "metaData": {},
                        "echoedMessage": {
                            "messageType": "ORDER_CREATED"
                        }
                    }
                    """;

            var orderCreatedType = "ORDER_CREATED";

            assertAll(
                () -> assertThat(typeParser.findAllTypes(json)).contains("ECHO_MESSAGE", orderCreatedType),
                () -> assertThat(typeParser.findType(json)).isEqualTo(orderCreatedType),
                () -> assertThat(typeParser.findTypeSafe(json)).contains(orderCreatedType),
                () -> assertThat(typeParser.isEchoedMessage(json)).isTrue()
            );
        }
    }

    @Nested
    class WhenTypeInMetaData {

        @Test
        void shouldFindMessageTypeOfNormalMessage() {
            var json = """
                {
                    "metaData": {
                        "type": "ORDER_CREATED"
                    }
                }
                """;

            var orderCreatedType = "ORDER_CREATED";

            assertAll(
                () -> assertThat(typeParser.findAllTypes(json)).containsOnly(orderCreatedType),
                () -> assertThat(typeParser.findType(json)).isEqualTo(orderCreatedType),
                () -> assertThat(typeParser.findTypeSafe(json)).contains(orderCreatedType),
                () -> assertThat(typeParser.isEchoedMessage(json)).isFalse()
            );
        }

        @Test
        void shouldFindMessageTypeOfEchoedMessage() {
            var json = """
                {
                    "metaData": {
                        "type": "ECHO_MESSAGE"
                    },
                    "echoedMessage": {
                        "metaData": {
                            "type": "ORDER_CREATED"
                        }
                    }
                }
                """;

        var orderCreatedType = "ORDER_CREATED";

        assertAll(
            () -> assertThat(typeParser.findAllTypes(json)).contains("ECHO_MESSAGE", orderCreatedType),
            () -> assertThat(typeParser.findType(json)).isEqualTo(orderCreatedType),
            () -> assertThat(typeParser.findTypeSafe(json)).contains(orderCreatedType),
            () -> assertThat(typeParser.isEchoedMessage(json)).isTrue()
        );
        }
    }

    @Nested
    class ShouldThrowMessageTypeParsingException {

        @Test
        void whenGivenMalformedJson() {
            // no ending curly brace
            var json = """
                    {
                        "messageType": null
                    """;

            assertThatExceptionOfType(MessageTypeParsingException.class)
                    .isThrownBy(() -> typeParser.findType(json))
                    .havingCause()
                    .isExactlyInstanceOf(RuntimeJsonException.class);

            assertThat(typeParser.findTypeSafe(json)).isEmpty();
        }

        @Test
        void whenInputIsNotJson() {
            var text = "Not even json";

            assertThatExceptionOfType(MessageTypeParsingException.class)
                    .isThrownBy(() -> typeParser.findType(text))
                    .havingCause()
                    .isExactlyInstanceOf(RuntimeJsonException.class);

            assertThat(typeParser.findTypeSafe(text)).isEmpty();
        }
    }

    @Nested
    class InternalExtractMessageType {

        @Test
        void shouldThrowVerifyException_WhenGivenMoreThanOneMessageType() {
            var json = """
                    {
                        "messageType": "TYPE_1",
                        "metaData": {
                            "type": "TYPE_2"
                        }
                    }
                    """;
            var messageTypes = Set.of("TYPE_1", "TYPE_2");

            assertThatExceptionOfType(VerifyException.class)
                    .isThrownBy(() -> MessageTypeParser.extractMessageType(messageTypes, json))
                    .withMessage("There is more than one message type: %s in message: %s", messageTypes, json);
        }
    }
}

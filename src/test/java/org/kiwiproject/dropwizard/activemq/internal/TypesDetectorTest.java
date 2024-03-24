package org.kiwiproject.dropwizard.activemq.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.kiwiproject.dropwizard.activemq.ActiveMqMessage.ContentType;
import org.kiwiproject.dropwizard.activemq.util.MessageTypeParser;

@DisplayName("TypesDetector")
class TypesDetectorTest {

    @Nested
    class DetermineContentTypeOf {

        @Test
        void shouldDetectJson() {
            var payload = """
                    {
                        "messageType": "TESTING",
                        "answer": 42
                    }
                    """;

            assertThat(TypesDetector.determineContentTypeOf(payload)).isEqualTo(ContentType.JSON);
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "not json",
                "<root><answer>42</answer></root>",
                "c,s,v"
        })
        void shouldDetectText(String payload) {
            assertThat(TypesDetector.determineContentTypeOf(payload)).isEqualTo(ContentType.TEXT);
        }
    }

    @Nested
    class DetermineMessageTypeFromPayload {

        @Test
        void shouldDetectMessageTypeInJson() {
            var payload = """
                    {
                        "messageType": "TESTING",
                        "answer": 42
                    }
                    """;

            assertThat(TypesDetector.determineMessageTypeFrom(payload)).isEqualTo("TESTING");
        }

        @Test
        void shouldBeUnknown_WhenJsonDoesNotContainMessageType() {
            var payload = """
                    {
                        "answer": 42
                    }
                    """;

            assertThat(TypesDetector.determineMessageTypeFrom(payload))
                    .isEqualTo(MessageTypeParser.UNKNOWN_MESSAGE_TYPE);
        }

        @Test
        void shouldBeTextMessage_WhenNotJson() {
            var payload = """
                    <response>
                        <status>200</status>
                        <text>The thing was done</text>
                    </response>
                    """;

            assertThat(TypesDetector.determineMessageTypeFrom(payload))
                    .isEqualTo(ContentType.TEXT.convertToMessageType());
        }
    }
}

package org.kiwiproject.dropwizard.activemq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.kiwiproject.dropwizard.activemq.exception.ActiveMqMessageException;
import org.kiwiproject.dropwizard.activemq.exception.ActiveMqMessageInvalidMessageTypeException;
import org.kiwiproject.dropwizard.activemq.exception.ActiveMqMessageMissingBodyException;
import org.kiwiproject.dropwizard.activemq.util.MessageTypeParser;

@DisplayName("ActiveMqConsumer")
class ActiveMqConsumerTest {

    private ActiveMqConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new DummyConsumer();
    }

    @Test
    void shouldHandleExceptionsWithoutThrowing() {
        var message = ActiveMqMessage.builder()
                .body("""
                        {
                            "messageType": "TESTING",
                        }
                        """)
                .build();

        var ex = new ActiveMqMessageException("THE_CATEGORY", "uh oh", new RuntimeException("Test failed!"));

        assertThatCode(() -> consumer.handleException(message, ex)).doesNotThrowAnyException();
    }

    @Test
    void shouldConsumeMessagesByDefault() {
        var message = ActiveMqMessage.builder().build();

        assertThat(consumer.shouldConsume(message)).isTrue();
    }

    @Nested
    class RequireValidBody {

        @Test
        void shouldReturnBody_WhenItExistsInTheMessage() {
            var text = """
                    {
                        "messageType": "TESTING",
                    }
                    """;
            var message = ActiveMqMessage.builder().body(text).build();

            assertThat(consumer.requireValidBody(message)).isEqualTo(text);
        }

        @Test
        void shouldThrowException_WhenBodyIsMissing() {
            var message = ActiveMqMessage.builder().build();

            assertThatExceptionOfType(ActiveMqMessageMissingBodyException.class)
                    .isThrownBy(() -> consumer.requireValidBody(message))
                    .withMessage("No body attached to ActiveMqMessage");
        }
    }

    @Nested
    class RequireValidMessageType {

        @Test
        void shouldReturnTheMessageType_WhenItExists() {
            var message = ActiveMqMessage.builder()
                    .messageType("TESTING")
                    .build();

            assertThat(consumer.requireValidMessageType(message)).isEqualTo("TESTING");
        }

        @Test
        void shouldThrowException_WhenMessageTypeIsMissing() {
            var message = ActiveMqMessage.builder().build();

            assertThatExceptionOfType(ActiveMqMessageInvalidMessageTypeException.class)
                    .isThrownBy(() -> consumer.requireValidMessageType(message))
                    .withMessage("No message type present");
        }

        @Test
        void shouldThrowException_WhenMessageTypeIsTheUnknownType() {
            var message = ActiveMqMessage.builder()
                    .messageType(MessageTypeParser.UNKNOWN_MESSAGE_TYPE)
                    .build();

            assertThatExceptionOfType(ActiveMqMessageInvalidMessageTypeException.class)
                    .isThrownBy(() -> consumer.requireValidMessageType(message))
                    .withMessage("Type parser was unable to identify the message type");
        }
    }

    static class DummyConsumer implements ActiveMqConsumer {
        @Override
        public Result consume(ActiveMqMessage message) {
            return Result.CONSUMED;
        }
    }
}

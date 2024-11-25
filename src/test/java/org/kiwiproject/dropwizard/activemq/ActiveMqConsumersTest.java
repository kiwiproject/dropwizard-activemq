package org.kiwiproject.dropwizard.activemq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.kiwiproject.curator.leader.ManagedLeaderLatch;
import org.kiwiproject.dropwizard.activemq.ActiveMqConsumer.Result;
import org.kiwiproject.dropwizard.activemq.test.util.ActiveMqMessages;

import java.util.function.Function;
import java.util.function.Predicate;

@DisplayName("ActiveMqConsumers")
class ActiveMqConsumersTest {

    private ManagedLeaderLatch leaderLatch;

    @BeforeEach
    void setUp() {
        leaderLatch = mock(ManagedLeaderLatch.class);
    }

    @Test
    void shouldCreateNewConsumer() {
        Function<ActiveMqMessage, Result> messageHandler = activeMqMessage -> Result.IGNORED;

        var consumer = ActiveMqConsumers.newConsumer(messageHandler);

        assertAll(
                () -> assertThat(consumer.shouldConsume(null)).isTrue(),
                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isTrue(),
                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isTrue(),

                () -> assertThat(consumer.consume(newActiveMqMessage())).isEqualTo(Result.IGNORED),
                () -> assertThat(consumer.consume(newActiveMqMessage())).isEqualTo(Result.IGNORED)
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false})
    void shouldCreateNewConsumer_WithPerMessageLogic(boolean shouldConsume) {
        Function<ActiveMqMessage, Result> messageHandler = activeMqMessage -> Result.CONSUMED;
        Predicate<ActiveMqMessage> messageChecker = activeMqMessage -> shouldConsume;

        var consumer = ActiveMqConsumers.newConsumer(messageHandler, messageChecker);

        assertAll(
                () -> assertThat(consumer.shouldConsume(null)).isEqualTo(shouldConsume),
                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isEqualTo(shouldConsume),
                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isEqualTo(shouldConsume),

                () -> assertThat(consumer.consume(newActiveMqMessage())).isEqualTo(Result.CONSUMED),
                () -> assertThat(consumer.consume(newActiveMqMessage())).isEqualTo(Result.CONSUMED)
        );
    }

    @Test
    void shouldCreateLeaderLatchConsumer_WhenIsLeader() {
        when(leaderLatch.hasLeadership()).thenReturn(true);

        Function<ActiveMqMessage, Result> messageHandler = activeMqMessage -> Result.CONSUMED;

        var consumer = ActiveMqConsumers.newLeaderLatchConsumer(leaderLatch, messageHandler);

        assertAll(
                () -> assertThat(consumer.shouldConsumeMessage(newActiveMqMessage())).isTrue(),
                () -> assertThat(consumer.shouldConsumeMessage(newActiveMqMessage())).isTrue(),

                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isTrue(),
                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isTrue(),

                () -> assertThat(consumer.consume(newActiveMqMessage())).isEqualTo(Result.CONSUMED),
                () -> assertThat(consumer.consume(newActiveMqMessage())).isEqualTo(Result.CONSUMED)
        );
    }

    @Test
    void shouldCreateLeaderLatchConsumer_WhenIsNotLeader() {
        when(leaderLatch.hasLeadership()).thenReturn(false);

        Function<ActiveMqMessage, Result> messageHandler = activeMqMessage -> Result.CONSUMED;

        var consumer = ActiveMqConsumers.newLeaderLatchConsumer(leaderLatch, messageHandler);

        assertAll(
                () -> assertThat(consumer.shouldConsumeMessage(newActiveMqMessage())).isTrue(),
                () -> assertThat(consumer.shouldConsumeMessage(newActiveMqMessage())).isTrue(),

                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isFalse(),
                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isFalse(),

                () -> assertThat(consumer.consume(newActiveMqMessage())).isEqualTo(Result.CONSUMED),
                () -> assertThat(consumer.consume(newActiveMqMessage())).isEqualTo(Result.CONSUMED)
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false})
    void shouldCreateLeaderLatchConsumer_WithPerMessageLogic_WhenIsLeader(boolean shouldConsumeMessage) {
        when(leaderLatch.hasLeadership()).thenReturn(true);

        Function<ActiveMqMessage, Result> messageHandler = activeMqMessage -> Result.CONSUMED;
        Predicate<ActiveMqMessage> messageChecker = activeMqMessage -> shouldConsumeMessage;

        var consumer = ActiveMqConsumers.newLeaderLatchConsumer(leaderLatch, messageHandler, messageChecker);

        assertAll(
                () -> assertThat(consumer.shouldConsumeMessage(newActiveMqMessage())).isEqualTo(shouldConsumeMessage),
                () -> assertThat(consumer.shouldConsumeMessage(newActiveMqMessage())).isEqualTo(shouldConsumeMessage),

                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isEqualTo(shouldConsumeMessage),
                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isEqualTo(shouldConsumeMessage),

                () -> assertThat(consumer.consume(newActiveMqMessage())).isEqualTo(Result.CONSUMED),
                () -> assertThat(consumer.consume(newActiveMqMessage())).isEqualTo(Result.CONSUMED)
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false})
    void shouldCreateLeaderLatchConsumer_WithPerMessageLogic_WhenIsNotLeader(boolean shouldConsumeMessage) {
        when(leaderLatch.hasLeadership()).thenReturn(false);

        Function<ActiveMqMessage, Result> messageHandler = activeMqMessage -> Result.CONSUMED;
        Predicate<ActiveMqMessage> messageChecker = activeMqMessage -> shouldConsumeMessage;

        var consumer = ActiveMqConsumers.newLeaderLatchConsumer(leaderLatch, messageHandler, messageChecker);

        assertAll(
                () -> assertThat(consumer.shouldConsumeMessage(newActiveMqMessage())).isEqualTo(shouldConsumeMessage),
                () -> assertThat(consumer.shouldConsumeMessage(newActiveMqMessage())).isEqualTo(shouldConsumeMessage),

                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isFalse(),
                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isFalse(),

                () -> assertThat(consumer.consume(newActiveMqMessage())).isEqualTo(Result.CONSUMED),
                () -> assertThat(consumer.consume(newActiveMqMessage())).isEqualTo(Result.CONSUMED)
        );
    }

    private ActiveMqMessage newActiveMqMessage() {
        return ActiveMqMessages.newJsonActiveMqMessage("""
                {
                    "message": "howdy!",
                    "timestamp": %d
                }
                """.formatted(System.currentTimeMillis()));
    }
}

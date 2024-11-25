package org.kiwiproject.dropwizard.activemq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.kiwiproject.curator.leader.ManagedLeaderLatch;
import org.kiwiproject.dropwizard.activemq.test.util.ActiveMqMessages;

@DisplayName("AbstractLeaderLatchConsumer")
@Slf4j
class AbstractLeaderLatchConsumerTest {

    private AbstractLeaderLatchConsumer consumer;
    private ManagedLeaderLatch leaderLatch;

    @BeforeEach
    void setUp() {
        leaderLatch = mock(ManagedLeaderLatch.class);
        consumer = new AbstractLeaderLatchConsumer(leaderLatch) {
            @Override
            public Result consume(ActiveMqMessage message) {
                return Result.CONSUMED;
            }
        };
    }

    @Test
    void shouldConsume_ShouldReturnTrue_WhenHasLeadership() {
        when(leaderLatch.hasLeadership()).thenReturn(true);

        assertAll(
                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isTrue(),
                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isTrue(),
                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isTrue()
        );

        verify(leaderLatch, times(3)).hasLeadership();
    }

    @Test
    void shouldConsume_ShouldReturnFalse_WhenDoesNotHaveLeadership() {
        when(leaderLatch.hasLeadership()).thenReturn(false);

        assertAll(
                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isFalse(),
                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isFalse(),
                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isFalse()
        );

        verify(leaderLatch, times(3)).hasLeadership();
    }

    @Test
    void shouldConsume_ShouldReturnFalse_WhenHasLeadership_ButShouldConsumeMessage_ReturnsFalse() {
        when(leaderLatch.hasLeadership()).thenReturn(true);

        consumer = new AbstractLeaderLatchConsumer(leaderLatch) {
            @Override
            public Result consume(ActiveMqMessage message) {
                return Result.CONSUMED;
            }

            @Override
            protected boolean shouldConsumeMessage(ActiveMqMessage message) {
                LOG.debug("Rejecting specific message!");
                return false;
            }
        };

        assertAll(
                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isFalse(),
                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isFalse(),
                () -> assertThat(consumer.shouldConsume(newActiveMqMessage())).isFalse()
        );

        verify(leaderLatch, times(3)).hasLeadership();
    }

    @Test
    void shouldConsumeMessage_ShouldConsumeAllMessagesByDefault() {
        assertAll(
            () -> assertThat(consumer.shouldConsumeMessage(null)).isTrue(),
            () -> assertThat(consumer.shouldConsumeMessage(newActiveMqMessage())).isTrue()
        );

        verifyNoInteractions(leaderLatch);
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

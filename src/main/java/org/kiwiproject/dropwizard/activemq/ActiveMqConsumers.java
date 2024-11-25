package org.kiwiproject.dropwizard.activemq;

import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;

import lombok.experimental.UtilityClass;

import org.kiwiproject.curator.leader.ManagedLeaderLatch;
import org.kiwiproject.dropwizard.activemq.ActiveMqConsumer.Result;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Provides factory methods to create {@link ActiveMqConsumer} instances from
 * functions.
 */
@UtilityClass
public class ActiveMqConsumers {

    /**
     * Create a new {@link ActiveMqConsumer} that processes messages using the given
     * function.
     *
     * @param messageProcessor the function that will process messages
     * @return a new instance
     */
    public static ActiveMqConsumer newConsumer(Function<ActiveMqMessage, Result> messageProcessor) {
        checkArgumentNotNull(messageProcessor);

        return messageProcessor::apply;
    }

    /**
     * Create a new {@link ActiveMqConsumer} that processes messages using the given
     * function.
     * when the message checker returns true.
     *
     * @param messageProcessor the function that will process messages
     * @param messageChecker   a predicate that determines if a specific message
     *                         should be consumed
     * @return a new instance
     */
    public static ActiveMqConsumer newConsumer(Function<ActiveMqMessage, Result> messageProcessor,
            Predicate<ActiveMqMessage> messageChecker) {

        checkArgumentNotNull(messageProcessor);
        checkArgumentNotNull(messageChecker);

        return new ActiveMqConsumer() {

            @Override
            public Result consume(ActiveMqMessage message) {
                return messageProcessor.apply(message);
            }

            @Override
            public boolean shouldConsume(ActiveMqMessage message) {
                return messageChecker.test(message);
            }
        };
    }

    /**
     * Create a new {@link ActiveMqConsumer} that processes messages using the given
     * function
     * when the leader latch is the leader.
     *
     * @param leaderLatch      the leader latch
     * @param messageProcessor the function that will process messages
     * @return a new instance
     * @see AbstractLeaderLatchConsumer
     */
    public static AbstractLeaderLatchConsumer newLeaderLatchConsumer(ManagedLeaderLatch leaderLatch,
            Function<ActiveMqMessage, Result> messageProcessor) {

        checkArgumentNotNull(leaderLatch);
        checkArgumentNotNull(messageProcessor);

        return new AbstractLeaderLatchConsumer(leaderLatch) {

            @Override
            public Result consume(ActiveMqMessage message) {
                return messageProcessor.apply(message);
            }
        };
    }

    /**
     * Create a new {@link ActiveMqConsumer} that processes messages using the given
     * function
     * when the leader latch is the leader <em>and</em> the message checker returns
     * true.
     * This allows for a per-message decision whether it should be consumed when the
     * leader
     * latch is the leader.
     *
     * @param leaderLatch      the leader latch
     * @param messageProcessor the function that will process messages
     * @param messageChecker   a predicate that determines if a specific message
     *                         should be consumed
     * @return a new instance
     * @see AbstractLeaderLatchConsumer
     */
    public static AbstractLeaderLatchConsumer newLeaderLatchConsumer(ManagedLeaderLatch leaderLatch,
            Function<ActiveMqMessage, Result> messageProcessor,
            Predicate<ActiveMqMessage> messageChecker) {

        checkArgumentNotNull(leaderLatch);
        checkArgumentNotNull(messageProcessor);
        checkArgumentNotNull(messageChecker);

        return new AbstractLeaderLatchConsumer(leaderLatch) {

            @Override
            public Result consume(ActiveMqMessage message) {
                return messageProcessor.apply(message);
            }

            @Override
            public boolean shouldConsumeMessage(ActiveMqMessage message) {
                return messageChecker.test(message);
            }
        };
    }
}

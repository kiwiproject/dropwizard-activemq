package org.kiwiproject.dropwizard.activemq;

import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;

import org.kiwiproject.curator.leader.ManagedLeaderLatch;

/**
 * Base class for {@link ActiveMqConsumer}s that want to use a {@link ManagedLeaderLatch}
 * when determining whether a message should be consumed. The default behavior is
 * that messages will be consumed only when the leader latch is the leader.
 * <p>
 * You can further customize message consumption by overriding the
 * {@link #shouldConsumeMessage(ActiveMqMessage)} method, which allows for
 * per-message logic. Assuming the leader latch has leadership, this method
 * then determines whether a message should be consumed. The default behavior
 * is to process every message so that implementing classes don't need to
 * override this if they only want message consumption to be based on the
 * leader latch.
 * <p>
 * The main reason you might want to use this class is to ensure messages from a
 * traditional JMS topic (not an ActiveMQ Virtual Topic) are only processed once.
 * The main downside is that messages are not load-balanced across all
 * Leader Latch participants (like an ActiveMQ Virtual Topic would be). This might
 * be an acceptable tradeoff in some situations, e.g., applications that process
 * a relatively low volume of messages from a regular JMS topic and that need to
 * ensure each message is processed only once.
 * <p>
 * When using this class, {@code dropwizard-leader-latch} is required.
 * By default, it is a provided-scope dependency and won't be included.
 */
public abstract class AbstractLeaderLatchConsumer implements ActiveMqConsumer {

    private final ManagedLeaderLatch leaderLatch;

    /**
     * Create a new instance. Subclasses must call this constructor.
     *
     * @param leaderLatch the Leader Latch to use
     */
    protected AbstractLeaderLatchConsumer(ManagedLeaderLatch leaderLatch) {
        this.leaderLatch = requireNotNull(leaderLatch, "leaderLatch must not be null");
    }

    /**
     * Checks whether the {@link ManagedLeaderLatch} is the leader and that
     * {@link #shouldConsumeMessage(ActiveMqMessage)} returns {@code true}.
     *
     * @param message the incoming {@link ActiveMqMessage}
     * @return {@code true} if the message should be consumed, otherwise {@code false}
     */
    @Override
    public boolean shouldConsume(ActiveMqMessage message) {
        return leaderLatch.hasLeadership() && shouldConsumeMessage(message);
    }

    /**
     * Override this to add custom per-message logic. The default is {@code true}.
     * <p>
     * Note that this is only called if the {@link ManagedLeaderLatch} is the leader.
     *
     * @param message the incoming {@link ActiveMqMessage}
     * @return {@code true} if the message should be consumed, otherwise {@code false}
     */
    protected boolean shouldConsumeMessage(ActiveMqMessage message) {
        return true;
    }
}

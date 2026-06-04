package org.kiwiproject.dropwizard.activemq.testing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.collect.KiwiLists.isNotNullOrEmpty;
import static org.kiwiproject.collect.KiwiLists.isNullOrEmpty;
import static org.kiwiproject.collect.KiwiLists.last;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.Pair;
import org.kiwiproject.dropwizard.activemq.ActiveMqProducer;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A fake (test double) implementation of {@link ActiveMqProducer}.
 *
 * @implNote This class is not thread-safe, i.e., from multiple threads simultaneously producing messages.
 */
public class FakeActiveMqProducer implements ActiveMqProducer {

    private static final String DEFAULT_ALL_EVENTS_QUEUE = "queue:" + ActiveMqConfig.DEFAULT_ALL_EVENTS_QUEUE_NAME;

    private final String allEventsQueue;
    private final ListMultimap<String, FakeJmsMessage> messages = ArrayListMultimap.create();
    private final ListMultimap<String, FakeJmsMessage> bytesMessages = ArrayListMultimap.create();
    private final ListMultimap<String, FakeJmsMessage> allEventsMessages = ArrayListMultimap.create();

    /**
     * Creates a new instance using the default all-events queue ({@code "queue:all_events"}).
     */
    public FakeActiveMqProducer() {
        this(DEFAULT_ALL_EVENTS_QUEUE);
    }

    /**
     * Creates a new instance using a custom all-events queue destination string.
     *
     * @param allEventsQueue the full destination string, e.g. {@code "queue:my_events"}
     */
    public FakeActiveMqProducer(String allEventsQueue) {
        this.allEventsQueue = requireNotBlank(allEventsQueue);
    }

    @Override
    public void produceToAllEventsQueue(String payload) {
        produce(allEventsQueue, payload, PayloadDestination.SPECIFIED_ONLY);
    }

    /**
     * Produce a message to the given destination.
     */
    @Override
    public void produce(String destination,
                        String payload,
                        PayloadDestination payloadDestination,
                        Map<String, Object> headers) {

        if (allEventsQueue.equals(destination)) {
            produceInternal(destination, payload, headers, allEventsMessages);
        } else {
            produceInternal(destination, payload, headers, messages);
        }

        if (payloadDestination == PayloadDestination.SPECIFIED_AND_ALL_EVENTS) {
            produceInternal(allEventsQueue, payload, headers, allEventsMessages);
        }
    }

    /**
     * Produce a bytes message to the given destination, encoding the payload as Base64.
     */
    @Override
    public void produceBytesMessage(String destination, byte[] payload) {
        produceInternal(destination, encodeToBase64(payload), Map.of(), bytesMessages);
    }

    private static void produceInternal(String destination,
                                        String payload,
                                        Map<String, Object> headers,
                                        Multimap<String, FakeJmsMessage> messages) {

        var fakeJmsMessage = FakeJmsMessage.of(payload, headers);
        messages.put(destination, fakeJmsMessage);
    }

    public static String encodeToBase64(byte[] payload) {
        return Base64.getEncoder().encodeToString(payload);
    }

    public static byte[] decodeFromBase64(String base64Payload) {
        return Base64.getDecoder().decode(base64Payload);
    }

    public static String decodeFromBase64ToUTF8String(String base64Payload) {
        return new String(decodeFromBase64(base64Payload), UTF_8);
    }

    /**
     * Clear all producer history.
     */
    public void clear() {
        clearMessages();
        clearAllEventsMessages();
        clearBytesMessages();
    }

    /**
     * Clear regular messages (non-byte).
     */
    public void clearMessages() {
        messages.clear();
    }

    /**
     * Clear messages in "all events" queue.
     */
    public void clearAllEventsMessages() {
        allEventsMessages.clear();
    }

    /**
     * Clears byte messages.
     */
    public void clearBytesMessages() {
        bytesMessages.clear();
    }

    /**
     * Returns all messages produced to all destinations.
     * <p>
     * Note that there is no guarantee about topic ordering. Messages for a given topic will appear
     * in order, but messages for topic "A" may or may not appear before messages for topic "B".
     * You can use {@link #history(String)} to get the history for a specific destination.
     *
     * @return all messages
     */
    public List<String> history() {
        return history(messages);
    }

    public List<String> history(String destination) {
        return history(destination, messages);
    }

    public boolean hasProducedTo(String destination) {
        return isNotNullOrEmpty(history(destination));
    }

    public Optional<String> lastMessageProducedToOrEmpty(String destination) {
        var history = history(destination);
        return isNullOrEmpty(history) ? Optional.empty() : Optional.of(last(history));
    }

    /**
     * @throws RuntimeException if no messages have been produced to destination
     */
    public String lastMessageProducedTo(String destination) {
        return last(history(destination));
    }

    public List<String> allEventsHistory() {
        return history(allEventsMessages);
    }

    public boolean hasProducedToAllEvents() {
        return isNotNullOrEmpty(allEventsHistory());
    }

    public Optional<String> lastMessageProducedToAllEventsOrEmpty() {
        var allEventsHistory = allEventsHistory();
        return isNullOrEmpty(allEventsHistory) ? Optional.empty() : Optional.of(last(allEventsHistory));
    }

    /**
     * @throws RuntimeException if no messages have been produced to the "All Events" queue
     */
    public String lastMessageProducedToAllEvents() {
        return last(allEventsHistory());
    }

    /**
     * Returns all bytes messages.
     * <p>
     * The same warnings about ordering noted for {@link #history()} apply here as well.
     */
    public List<String> bytesHistory() {
        return history(bytesMessages);
    }

    public List<String> bytesHistory(String destination) {
        return history(destination, bytesMessages);
    }

    public List<Pair<String, Map<String, Object>>> headersHistory() {
        return messages.values()
                .stream()
                .map(entry -> Pair.of(entry.getPayload(), entry.getHeaders()))
                .toList();
    }

    private static List<String> history(Multimap<String, FakeJmsMessage> messages) {
        return messages.values().stream()
                .map(FakeJmsMessage::getPayload)
                .toList();
    }

    private static List<String> history(String destination, Multimap<String, FakeJmsMessage> messages) {
        checkArgumentNotBlank(destination);
        checkArgumentNotNull(messages);

        return messages.get(destination).stream()
                .map(FakeJmsMessage::getPayload)
                .toList();
    }
}

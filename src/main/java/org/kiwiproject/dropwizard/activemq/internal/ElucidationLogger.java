package org.kiwiproject.dropwizard.activemq.internal;

import static java.util.Objects.nonNull;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.kiwiproject.elucidation.client.ElucidationResult;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class ElucidationLogger implements BiConsumer<ElucidationResult, Throwable> {

    static final ElucidationLogger INSTANCE = new ElucidationLogger();

    private static final String UNKNOWN_REASON = "[unknown reason]";

    private final AtomicLong successCount = new AtomicLong();
    private final AtomicLong errorCount = new AtomicLong();
    private final AtomicLong skipCount = new AtomicLong();
    private final AtomicLong failureCount = new AtomicLong();

    static void logResult(ElucidationResult result, Throwable throwable) {
        INSTANCE.accept(result, throwable);
    }

    @Override
    public void accept(ElucidationResult result, Throwable throwable) {
        if (nonNull(throwable)) {
            onFailure(throwable);
        } else {
            onSuccess(result);
        }
    }

    private void onFailure(Throwable throwable) {
        var count = failureCount.incrementAndGet();
        LOG.warn("There was a problem recording an event to elucidation ({})", count, throwable);
    }

    private void onSuccess(ElucidationResult result) {
        switch (result.getStatus()) {
            case SUCCESS -> {
                var count = successCount.incrementAndGet();
                LOG.debug("Successfully recorded event to elucidation ({})", count);
            }

            case ERROR -> {
                var count = errorCount.incrementAndGet();
                LOG.warn("There was a problem recording an event to elucidation ({}). Reason: {}",
                        count,
                        result.getErrorMessage().orElse(UNKNOWN_REASON),
                        result.getException().orElse(null));  // SLF4J will gracefully handle an Exception or null
            }

            case SKIPPED -> {
                var count = skipCount.incrementAndGet();
                LOG.info("Skipped recording elucidation event ({}). Reason: {}",
                        count,
                        result.getSkipMessage().orElse(UNKNOWN_REASON));
            }
        }
    }

    long getSuccessCount() {
        return successCount.get();
    }

    long getErrorCount() {
        return errorCount.get();
    }

    long getSkipCount() {
        return skipCount.get();
    }

    long getFailureCount() {
        return failureCount.get();
    }

    void resetCounts() {
        successCount.set(0);
        errorCount.set(0);
        skipCount.set(0);
        failureCount.set(0);
    }
}

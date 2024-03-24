package org.kiwiproject.dropwizard.activemq.internal;

import static java.util.Objects.nonNull;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.kiwiproject.elucidation.client.ElucidationResult;

import java.util.function.BiConsumer;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class ElucidationLogger implements BiConsumer<ElucidationResult, Throwable> {

    private static final ElucidationLogger INSTANCE = new ElucidationLogger();

    private static final String UNKNOWN_REASON = "[unknown reason]";

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

    private static void onFailure(Throwable throwable) {
        LOG.warn("There was a problem recording an event to elucidation", throwable);
    }

    private static void onSuccess(ElucidationResult result) {
        switch (result.getStatus()) {
            case SUCCESS -> LOG.debug("Successfully recorded event to elucidation");

            case ERROR -> LOG.warn("There was a problem recording an event to elucidation. Reason: {}",
                    result.getErrorMessage().orElse(UNKNOWN_REASON),
                    result.getException().orElse(null));  // SLF4J will gracfeully handle if this is an Exception or null

            case SKIPPED -> LOG.info("Skipped recording elucidation event. Reason: {}",
                    result.getSkipMessage().orElse(UNKNOWN_REASON));
        }
    }
}

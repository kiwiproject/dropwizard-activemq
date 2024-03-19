package org.kiwiproject.dropwizard.activemq.internal;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.kiwiproject.elucidation.client.ElucidationResult;

import java.util.function.BiConsumer;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class ElucidationLogger implements BiConsumer<ElucidationResult, Throwable> {

    // TODO

    static void logResult(ElucidationResult result, Throwable throwable) {
        // TODO
    }

    @Override
    public void accept(ElucidationResult result, Throwable throwable) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'accept'");
    }

}

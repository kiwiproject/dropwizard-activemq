package org.kiwiproject.dropwizard.activemq;

import lombok.Builder;
import lombok.Getter;

import org.kiwiproject.elucidation.client.ElucidationRecorder;
import org.kiwiproject.elucidation.common.model.ConnectionEvent;

import java.util.Optional;
import java.util.function.Function;

/**
 * Context object containing the Elucidation event recorder and the consuming
 * and producing functions that DropwizardActiveMq will use to transform messages
 * into Elucidation {@link ConnectionEvent} objects.
 * <p>
 * Generally this won't be needed by applications.
 */
@Getter
@Builder
public class ElucidationContext {

    private ElucidationRecorder eventRecorder;
    private Function<String, Optional<ConnectionEvent>> consumingTextMessageEventFactory;
    private Function<String, Optional<ConnectionEvent>> producingTextMessageEventFactory;
}

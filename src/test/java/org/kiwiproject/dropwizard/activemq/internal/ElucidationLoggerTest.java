package org.kiwiproject.dropwizard.activemq.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.kiwiproject.elucidation.client.ElucidationResult;

@DisplayName("ElucidationLogger")
class ElucidationLoggerTest {

    private ElucidationLogger instance;

    @BeforeEach
    void setUp() {
        instance = ElucidationLogger.INSTANCE;
        instance.resetCounts();
    }

    @Test
    void shouldRecordSuccessResult() {
        assertThat(instance.getSuccessCount()).isZero();

        ElucidationLogger.logResult(ElucidationResult.ok(), null);
        assertThat(instance.getSuccessCount()).isOne();

        ElucidationLogger.logResult(ElucidationResult.ok(), null);
        ElucidationLogger.logResult(ElucidationResult.ok(), null);
        assertThat(instance.getSuccessCount()).isEqualTo(3);

        assertThat(instance.getErrorCount()).isZero();
        assertThat(instance.getSkipCount()).isZero();
        assertThat(instance.getFailureCount()).isZero();
    }

    @Test
    void shouldRecordErrorResult() {
        assertThat(instance.getErrorCount()).isZero();

        ElucidationLogger.logResult(ElucidationResult.fromErrorMessage("oops"), null);
        assertThat(instance.getErrorCount()).isOne();

        ElucidationLogger.logResult(ElucidationResult.fromException(new RuntimeException("bad")), null);
        assertThat(instance.getErrorCount()).isEqualTo(2);

        assertThat(instance.getSuccessCount()).isZero();
        assertThat(instance.getSkipCount()).isZero();
        assertThat(instance.getFailureCount()).isZero();
    }

    @Test
    void shouldRecordSkipResult() {
        assertThat(instance.getSkipCount()).isZero();

        ElucidationLogger.logResult(ElucidationResult.fromSkipMessage("ignore 1"), null);
        assertThat(instance.getSkipCount()).isOne();

        ElucidationLogger.logResult(ElucidationResult.fromSkipMessage("ignore 2"), null);
        ElucidationLogger.logResult(ElucidationResult.fromSkipMessage("ignore 3"), null);
        ElucidationLogger.logResult(ElucidationResult.fromSkipMessage("ignore 4"), null);
        assertThat(instance.getSkipCount()).isEqualTo(4);

        assertThat(instance.getSuccessCount()).isZero();
        assertThat(instance.getErrorCount()).isZero();
        assertThat(instance.getFailureCount()).isZero();
    }

    @Test
    void shouldHandleFailures() {
        assertThat(instance.getFailureCount()).isZero();

        ElucidationLogger.logResult(null, new Exception("oop 1"));
        assertThat(instance.getFailureCount()).isOne();

        ElucidationLogger.logResult(null, new Exception("oop 2"));
        ElucidationLogger.logResult(null, new Exception("oop 3"));
        assertThat(instance.getFailureCount()).isEqualTo(3);

        assertThat(instance.getSuccessCount()).isZero();
        assertThat(instance.getErrorCount()).isZero();
        assertThat(instance.getSkipCount()).isZero();
    }
}

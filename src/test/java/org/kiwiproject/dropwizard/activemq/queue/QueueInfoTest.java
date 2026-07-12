package org.kiwiproject.dropwizard.activemq.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.kiwiproject.collect.KiwiMaps;

import java.util.LinkedHashMap;
import java.util.Map;

@DisplayName("QueueInfo")
class QueueInfoTest {

    @Test
    void constructor_ShouldThrowIllegalArgument_ForNullMessageTypeCounts() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new QueueInfo(true, 0, 0, 0, null))
                .withMessage("messageTypeCounts must not be null");
    }

    @Test
    void constructor_ShouldThrowIllegalArgument_ForInvalidMessageCounts() {
        assertAll(
                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> new QueueInfo(true, -1, 0, 0, Map.of()))
                        .withMessage("textMessageCount must be greater than or equal to zero"),

                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> new QueueInfo(true, 0, -1, 0, Map.of()))
                        .withMessage("bytesMessageCount must be greater than or equal to zero"),

                () -> assertThatIllegalArgumentException()
                        .isThrownBy(() -> new QueueInfo(true, 0, 0, -1, Map.of()))
                        .withMessage("otherMessageCount must be greater than or equal to zero")
        );
    }

    @Test
    void ofExists_ShouldSetAllFields() {
        var messageTypeCounts = Map.of("STATUS_CHANGE", 1);

        var queueInfo = QueueInfo.ofExists(1, 2, 3, messageTypeCounts);

        assertAll(
                () -> assertThat(queueInfo.exists()).isTrue(),
                () -> assertThat(queueInfo.textMessageCount()).isEqualTo(1),
                () -> assertThat(queueInfo.bytesMessageCount()).isEqualTo(2),
                () -> assertThat(queueInfo.otherMessageCount()).isEqualTo(3),
                () -> assertThat(queueInfo.messageTypeCounts()).containsExactlyEntriesOf(messageTypeCounts)
        );
    }

    @Test
    void ofEmpty_ShouldReturnExistingQueue_WithZeroCounts() {
        var queueInfo = QueueInfo.ofEmpty();

        assertAll(
                () -> assertThat(queueInfo.exists()).isTrue(),
                () -> assertThat(queueInfo.textMessageCount()).isZero(),
                () -> assertThat(queueInfo.bytesMessageCount()).isZero(),
                () -> assertThat(queueInfo.otherMessageCount()).isZero(),
                () -> assertThat(queueInfo.messageTypeCounts()).isEmpty()
        );
    }

    @Test
    void ofDoesNotExist_ShouldReturnNonExistentQueue_WithZeroCounts() {
        var queueInfo = QueueInfo.ofDoesNotExist();

        assertAll(
                () -> assertThat(queueInfo.exists()).isFalse(),
                () -> assertThat(queueInfo.textMessageCount()).isZero(),
                () -> assertThat(queueInfo.bytesMessageCount()).isZero(),
                () -> assertThat(queueInfo.otherMessageCount()).isZero(),
                () -> assertThat(queueInfo.messageTypeCounts()).isEmpty()
        );
    }

    @Test
    void messageTypeCounts_ShouldPreserveInsertionOrder() {
        var messageTypeCounts = KiwiMaps.<String, Integer>newLinkedHashMap(
                "STATUS_CHANGE", 42,
                "ORDER_STATUS_CHANGE", 12
        );

        var queueInfo = QueueInfo.ofExists(54, 0, 0, messageTypeCounts);

        assertThat(queueInfo.messageTypeCounts().keySet())
                .containsExactly("STATUS_CHANGE", "ORDER_STATUS_CHANGE");
    }

    @Test
    void messageTypeCounts_ShouldBeUnmodifiable() {
        var queueInfo = QueueInfo.ofExists(1, 0, 0, new LinkedHashMap<>(Map.of("STATUS_CHANGE", 1)));
        var messageTypeCounts = queueInfo.messageTypeCounts();

        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> messageTypeCounts.put("OTHER", 1));
    }

    @Test
    void messageTypeCounts_ShouldBeDefensivelyCopied() {
        var mutableMessageTypeCounts = new LinkedHashMap<>(Map.of("STATUS_CHANGE", 1));

        var queueInfo = QueueInfo.ofExists(1, 0, 0, mutableMessageTypeCounts);
        mutableMessageTypeCounts.put("OTHER", 1);

        assertThat(queueInfo.messageTypeCounts()).containsOnlyKeys("STATUS_CHANGE");
    }
}

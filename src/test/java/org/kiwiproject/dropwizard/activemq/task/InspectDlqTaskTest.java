package org.kiwiproject.dropwizard.activemq.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.kiwiproject.collect.KiwiMaps;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqHealthConfig;
import org.kiwiproject.dropwizard.activemq.queue.QueueInfo;
import org.kiwiproject.dropwizard.activemq.queue.QueueInspector;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

@DisplayName("InspectDlqTask")
class InspectDlqTaskTest {

    private QueueInspector inspector;
    private InspectDlqTask task;
    private String dlqName;
    private StringWriter sw;
    private PrintWriter pw;

    @BeforeEach
    void setUp() {
        inspector = mock(QueueInspector.class);
        dlqName = "My.DLQ";
        task = new InspectDlqTask(inspector, dlqName);
        sw = new StringWriter();
        pw = new PrintWriter(sw);
    }

    @Test
    void shouldUseDefaultDlqName_WhenConstructedWithSingleArgConstructor() {
        var defaultDlqTask = new InspectDlqTask(inspector);
        when(inspector.getQueueInfo(anyString())).thenReturn(QueueInfo.ofDoesNotExist());

        defaultDlqTask.execute(Map.of(), pw);

        verify(inspector, only()).getQueueInfo(ActiveMqHealthConfig.DEFAULT_DLQ_NAME);
    }

    @Test
    void shouldExecute_WhenDlqDoesNotExist() {
        when(inspector.getQueueInfo(anyString())).thenReturn(QueueInfo.ofDoesNotExist());

        task.execute(Map.of(), pw);

        assertThat(sw).hasToString("""
                DLQ does not exist
                """);

        verify(inspector, only()).getQueueInfo(dlqName);
    }

    @Test
    void shouldExecute_WhenDlqExists_ButHasNoMessages() {
        when(inspector.getQueueInfo(anyString())).thenReturn(QueueInfo.ofEmpty());

        task.execute(Map.of(), pw);

        assertThat(sw).hasToString("""
                textMessageCount: 0
                bytesMessageCount: 0
                otherMessageCount: 0
                """);

        verify(inspector, only()).getQueueInfo(dlqName);
    }

    @Test
    void shouldExecute_WhenDlqExists_WithNoKnownTextMessageTypes() {
        when(inspector.getQueueInfo(anyString())).thenReturn(QueueInfo.ofExists(1, 2, 1, Map.of()));

        task.execute(Map.of(), pw);

        assertThat(sw).hasToString("""
                textMessageCount: 1
                bytesMessageCount: 2
                otherMessageCount: 1
                
                Text message types:
                [no known message types]
                """);

        verify(inspector, only()).getQueueInfo(dlqName);
    }

    @Test
    void shouldExecute_WhenDlqExists_WithKnownTextMessageTypes() {
        var messageTypeCounts = KiwiMaps.<String, Integer>newLinkedHashMap(
                "STATUS_CHANGE", 42,
                "ORDER_STATUS_CHANGE", 12
        );
        when(inspector.getQueueInfo(anyString())).thenReturn(QueueInfo.ofExists(54, 0, 0, messageTypeCounts));

        task.execute(Map.of(), pw);

        assertThat(sw).hasToString("""
                textMessageCount: 54
                bytesMessageCount: 0
                otherMessageCount: 0
                
                Text message types:
                STATUS_CHANGE: 42
                ORDER_STATUS_CHANGE: 12
                """);

        verify(inspector, only()).getQueueInfo(dlqName);
    }

    @Test
    void shouldExecute_WhenDlqExists_WithNoTextMessages() {
        when(inspector.getQueueInfo(anyString())).thenReturn(QueueInfo.ofExists(0, 1, 0, Map.of()));

        task.execute(Map.of(), pw);

        assertThat(sw).hasToString("""
                textMessageCount: 0
                bytesMessageCount: 1
                otherMessageCount: 0
                """);

        verify(inspector, only()).getQueueInfo(dlqName);
    }
}

package org.kiwiproject.dropwizard.activemq.task;

import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;

import io.dropwizard.servlets.tasks.Task;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqHealthConfig;
import org.kiwiproject.dropwizard.activemq.queue.QueueInspector;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

/**
 * Dropwizard {@link Task} to inspect the ActiveMQ dead letter queue.
 */
public class InspectDlqTask extends Task {

    private final QueueInspector queueInspector;
    private final String dlqName;

    /**
     * Create a new instance using the default dead letter queue name, which is
     * {@link org.kiwiproject.dropwizard.activemq.config.ActiveMqHealthConfig#DEFAULT_DLQ_NAME}.
     *
     * @param queueInspector the {@link QueueInspector} to use
     */
    public InspectDlqTask(QueueInspector queueInspector) {
        this(queueInspector, ActiveMqHealthConfig.DEFAULT_DLQ_NAME);
    }

    /**
     * Create a new instance. This is useful if the dead letter queue
     * has a custom name.
     *
     * @param queueInspector the {@link QueueInspector} to use
     * @param dlqName        the name of the dead letter queue
     */
    public InspectDlqTask(QueueInspector queueInspector, String dlqName) {
        super("inspectDeadLetterQueue");
        this.queueInspector = requireNotNull(queueInspector);
        this.dlqName = requireNotBlank(dlqName);
    }

    @Override
    public void execute(Map<String, List<String>> parameters, PrintWriter output) {
        var dlqInfo = queueInspector.getQueueInfo(dlqName);

        if (!dlqInfo.exists()) {
            output.println("DLQ does not exist");
            return;
        }

        output.printf("textMessageCount: %d%n", dlqInfo.textMessageCount());
        output.printf("bytesMessageCount: %d%n", dlqInfo.bytesMessageCount());
        output.printf("otherMessageCount: %d%n", dlqInfo.otherMessageCount());

        if (dlqInfo.textMessageCount() > 0) {
            output.println();
            output.println("Text message types:");
            if (dlqInfo.messageTypeCounts().isEmpty()) {
                output.println("[no known message types]");
            } else {
                dlqInfo.messageTypeCounts().forEach((messageType, count) ->
                        output.printf("%s: %d%n", messageType, count));
            }
        }
    }
}

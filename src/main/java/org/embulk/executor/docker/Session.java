package org.embulk.executor.docker;

import com.github.kamatama41.nsocket.Connection;
import org.embulk.EmbulkEmbed;
import org.embulk.config.ConfigSource;
import org.embulk.config.ModelManager;
import org.embulk.config.TaskReport;
import org.embulk.spi.ExecSession;
import org.embulk.spi.ProcessTask;
import org.embulk.spi.util.Executors;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import static java.nio.charset.StandardCharsets.UTF_8;

class Session {
    private final String id;
    private final EmbulkEmbed embed;
    private final DockerExecutor.PluginTask pluginTask;
    private final ProcessTask processTask;
    private final ExecSession session;
    private final ModelManager modelManager;

    Session(
            String id, String systemConfig, String pluginTaskConfig, String processTaskConfig) {
        this.id = id;
        this.embed = newEmbulkBootstrap(systemConfig).initialize();
        this.modelManager = embed.getModelManager();
        this.pluginTask = modelManager.readObject(DockerExecutor.PluginTask.class, pluginTaskConfig);
        this.processTask = modelManager.readObject(ProcessTask.class, processTaskConfig);
        this.session = ExecSession.builder(embed.getInjector()).build();
    }

    void runTask(int taskIndex, Connection connection) {
        try {
            Executors.process(session, processTask, taskIndex, new Executors.ProcessStateCallback() {
                @Override
                public void started() {
                    connection.sendCommand("notifyTaskState", new UpdateTaskStateData(id, taskIndex, TaskState.STARTED));
                }

                @Override
                public void inputCommitted(TaskReport report) {
                    UpdateTaskStateData data = new UpdateTaskStateData(id, taskIndex, TaskState.INPUT_COMMIITTED);
                    data.setTaskReport(modelManager.writeObject(report));
                    connection.sendCommand("notifyTaskState", data);
                }

                @Override
                public void outputCommitted(TaskReport report) {
                    UpdateTaskStateData data = new UpdateTaskStateData(id, taskIndex, TaskState.OUTPUT_COMMITTED);
                    data.setTaskReport(modelManager.writeObject(report));
                    connection.sendCommand("notifyTaskState", data);
                }
            });
        } finally {
            connection.sendCommand("notifyTaskState", new UpdateTaskStateData(id, taskIndex, TaskState.FINISHED));
        }
    }

    private static EmbulkEmbed.Bootstrap newEmbulkBootstrap(String configJson) {
        ConfigSource systemConfig = getSystemConfig(configJson);
        return new EmbulkEmbed.Bootstrap().setSystemConfig(systemConfig);
    }

    private static ConfigSource getSystemConfig(String configJson) {
        try (InputStream in = new ByteArrayInputStream(configJson.getBytes(UTF_8))) {
            return EmbulkEmbed.newSystemConfigLoader().fromJson(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

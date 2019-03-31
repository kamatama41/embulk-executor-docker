package org.embulk.executor.remoteserver;

import com.github.kamatama41.nsocket.Connection;
import org.embulk.EmbulkEmbed;
import org.embulk.config.ConfigSource;
import org.embulk.config.ModelManager;
import org.embulk.config.TaskReport;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecSession;
import org.embulk.spi.ProcessTask;
import org.embulk.spi.util.Executors;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

class Session implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Session.class);
    private final String id;
    private final EmbulkEmbed embed;
    private final ScriptingContainer jruby;
    private final RemoteServerExecutor.PluginTask pluginTask;
    private final ProcessTask processTask;
    private final List<PluginArchive.GemSpec> gemSpecs;
    private final byte[] pluginArchive;
    private final ExecSession session;
    private final ModelManager modelManager;
    private final ConcurrentMap<Integer, Queue<UpdateTaskStateData>> bufferMap;
    private final ExecutorService sessionRunner;
    private volatile Connection connection;

    Session(
            String id,
            String systemConfig,
            String pluginTaskConfig,
            String processTaskConfig,
            List<PluginArchive.GemSpec> gemSpecs,
            byte[] pluginArchive
    ) {
        this.id = id;
        this.embed = newEmbulkBootstrap(systemConfig).initialize();
        this.jruby = embed.getInjector().getInstance(ScriptingContainer.class);
        this.modelManager = embed.getModelManager();
        this.pluginTask = modelManager.readObject(RemoteServerExecutor.PluginTask.class, pluginTaskConfig);
        this.processTask = modelManager.readObject(ProcessTask.class, processTaskConfig);
        this.gemSpecs = gemSpecs;
        this.pluginArchive = pluginArchive;
        this.session = ExecSession.builder(embed.getInjector()).build();
        loadPluginArchive();
        this.bufferMap = new ConcurrentHashMap<>();
        this.sessionRunner = java.util.concurrent.Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setName("session-runner-" + id);
            t.setDaemon(true);
            return t;
        });
    }

    void runTaskAsynchronously(int taskIndex) {
        sessionRunner.submit(() -> Exec.doWith(session, () -> {
            runTask(taskIndex);
            return null;
        }));
    }

    private void runTask(int taskIndex) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        bufferMap.putIfAbsent(taskIndex, new LinkedList<>());
        try {
            Executors.process(session, processTask, taskIndex, new Executors.ProcessStateCallback() {
                @Override
                public void started() {
                    sendCommand(taskIndex, new UpdateTaskStateData(id, taskIndex, TaskState.STARTED));
                }

                @Override
                public void inputCommitted(TaskReport report) {
                    UpdateTaskStateData data = new UpdateTaskStateData(id, taskIndex, TaskState.INPUT_COMMITTED);
                    data.setTaskReport(modelManager.writeObject(report));
                    sendCommand(taskIndex, data);
                }

                @Override
                public void outputCommitted(TaskReport report) {
                    UpdateTaskStateData data = new UpdateTaskStateData(id, taskIndex, TaskState.OUTPUT_COMMITTED);
                    data.setTaskReport(modelManager.writeObject(report));
                    sendCommand(taskIndex, data);
                }
            });
            sendCommand(taskIndex, new UpdateTaskStateData(id, taskIndex, TaskState.FINISHED));
        } catch (Exception e) {
            log.warn(String.format("Failed to run task[%d]", taskIndex), e);
            UpdateTaskStateData data = new UpdateTaskStateData(id, taskIndex, TaskState.FAILED);
            data.setErrorMessage(e.getMessage());
            sendCommand(taskIndex, data);
        }

        Queue<UpdateTaskStateData> buffer = bufferMap.get(taskIndex);
        if (buffer.isEmpty()) {
            return;
        }

        // Flush buffer if remaining
        int waitSeconds = 10;
        while (!buffer.isEmpty() && (System.currentTimeMillis() - startTime) <= pluginTask.getTimeoutSeconds() * 1000) {
            if (connection.isOpen()) {
                flushBuffer(taskIndex, connection);
                return;
            }
            log.warn("Connection is closed, wait {} seconds until reconnected.", waitSeconds);
            TimeUnit.SECONDS.sleep(waitSeconds);
        }
        log.warn("Connection was not able to be available again.");
    }

    void updateConnection(Connection connection) {
        this.connection = connection;
    }

    private void sendCommand(int taskIndex, UpdateTaskStateData data) {
        bufferMap.get(taskIndex).offer(data);
        if (!connection.isOpen()) {
            log.warn("Connection is closed, add data to buffer.");
            return;
        }
        flushBuffer(taskIndex, connection);
    }

    private void flushBuffer(int taskIndex, Connection connection) {
        UpdateTaskStateData data;
        Queue<UpdateTaskStateData> buffer = bufferMap.get(taskIndex);
        while ((data = buffer.poll()) != null) {
            connection.sendCommand(NotifyTaskStateCommand.ID, data);
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

    private void loadPluginArchive() {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(pluginArchive)) {
            Path gemsDir = Files.createTempDirectory("embulk_gems");
            PluginArchive.load(gemsDir.toFile(), gemSpecs, bis).restoreLoadPathsTo(jruby);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        log.debug("Closing the session {}", id);
        sessionRunner.shutdownNow();
    }
}

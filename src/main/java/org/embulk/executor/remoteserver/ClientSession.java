package org.embulk.executor.remoteserver;

import org.embulk.config.ModelManager;
import org.embulk.config.TaskReport;
import org.embulk.spi.ProcessState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

class ClientSession {
    private static final Logger log = LoggerFactory.getLogger(ClientSession.class);

    private final String id;
    private final String systemConfigJson;
    private final String pluginTaskJson;
    private final String processTaskJson;
    private final List<PluginArchive.GemSpec> gemSpecs;
    private final byte[] pluginArchiveBytes;

    private final ProcessState state;
    private final CountDownLatch timer;
    private final int inputTaskCount;
    private final ModelManager modelManager;
    private volatile boolean isFinished;
    private final Map<Integer, String> errorMessages;

    ClientSession(
            String systemConfigJson, String pluginTaskJson, String processTaskJson,
            List<PluginArchive.GemSpec> gemSpecs, byte[] pluginArchiveBytes,
            ProcessState state, int inputTaskCount, ModelManager modelManager) {
        this.id = UUID.randomUUID().toString();
        this.systemConfigJson = systemConfigJson;
        this.pluginTaskJson = pluginTaskJson;
        this.processTaskJson = processTaskJson;
        this.gemSpecs = gemSpecs;
        this.pluginArchiveBytes = pluginArchiveBytes;
        this.state = state;
        this.timer = new CountDownLatch(inputTaskCount);
        this.inputTaskCount = inputTaskCount;
        this.modelManager = modelManager;
        this.isFinished = false;
        this.errorMessages = new ConcurrentHashMap<>();
    }

    String getId() {
        return id;
    }

    String getSystemConfigJson() {
        return systemConfigJson;
    }

    String getPluginTaskJson() {
        return pluginTaskJson;
    }

    String getProcessTaskJson() {
        return processTaskJson;
    }

    List<PluginArchive.GemSpec> getGemSpecs() {
        return gemSpecs;
    }

    byte[] getPluginArchiveBytes() {
        return pluginArchiveBytes;
    }

    boolean isFinished() {
        return isFinished;
    }

    synchronized void update(UpdateTaskStateData data) {
        switch (data.getTaskState()) {
            case STARTED:
                state.getInputTaskState(data.getTaskIndex()).start();
                state.getOutputTaskState(data.getTaskIndex()).start();
                break;
            case INPUT_COMMITTED:
                state.getInputTaskState(data.getTaskIndex()).setTaskReport(getTaskReport(data.getTaskReport()));
                break;
            case OUTPUT_COMMITTED:
                state.getOutputTaskState(data.getTaskIndex()).setTaskReport(getTaskReport(data.getTaskReport()));
                break;
            case FAILED:
                errorMessages.put(data.getTaskIndex(), data.getErrorMessage());
                timer.countDown();
                break;
            case FINISHED:
                state.getInputTaskState(data.getTaskIndex()).finish();
                state.getOutputTaskState(data.getTaskIndex()).finish();
                timer.countDown();
                showProgress(state, inputTaskCount);
                break;
        }
    }

    void waitUntilCompleted(int timeoutSeconds) throws InterruptedException, TimeoutException {
        try {
            if (!timer.await(timeoutSeconds, TimeUnit.SECONDS)) {
                throw new TimeoutException(String.format("The session (%s) was time-out.", id));
            }
            if (!errorMessages.isEmpty()) {
                String message = errorMessages.entrySet().stream()
                        .map(e -> String.format("%d: %s", e.getKey(), e.getValue()))
                        .collect(Collectors.joining(System.lineSeparator()));
                throw new TaskExecutionException(message);
            }
        } finally {
            isFinished = true;
        }
    }

    private TaskReport getTaskReport(String json) {
        return modelManager.readObject(TaskReport.class, json);
    }

    private static void showProgress(ProcessState state, int taskCount) {
        int started = 0;
        int finished = 0;
        for (int i = 0; i < taskCount; i++) {
            if (state.getOutputTaskState(i).isStarted()) {
                started++;
            }
            if (state.getOutputTaskState(i).isFinished()) {
                finished++;
            }
        }
        log.info(String.format("{done:%3d / %d, running: %d}", finished, taskCount, started - finished));
    }
}

package org.embulk.executor.remoteserver;

import com.google.inject.Inject;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.ModelManager;
import org.embulk.config.Task;
import org.embulk.exec.ForSystemConfig;
import org.embulk.spi.ExecutorPlugin;
import org.embulk.spi.ProcessState;
import org.embulk.spi.ProcessTask;
import org.embulk.spi.Schema;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class RemoteServerExecutor implements ExecutorPlugin {
    private static final Logger log = LoggerFactory.getLogger(RemoteServerExecutor.class);
    private static final Host DEFAULT_HOST = new Host("localhost", 30000);
    private final ConfigSource systemConfig;
    private final ScriptingContainer jruby;

    interface PluginTask extends Task {
        @Config("hosts")
        @ConfigDefault("[]")
        List<Host> getHosts();

        @Config("timeout_seconds")
        @ConfigDefault("3600")
        int getTimeoutSeconds();

        @ConfigInject
        ModelManager getModelManager();
    }

    @Inject
    public RemoteServerExecutor(@ForSystemConfig ConfigSource systemConfig, ScriptingContainer jruby) {
        this.systemConfig = systemConfig;
        this.jruby = jruby;
    }

    @Override
    public void transaction(ConfigSource config, Schema outputSchema, int inputTaskCount, Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);
        if (task.getHosts().isEmpty()) {
            log.info("Hosts is empty. Run with a local server.");
            try (EmbulkServer _autoclosed = EmbulkServer.start(DEFAULT_HOST.getName(), DEFAULT_HOST.getPort(), 1)) {
                control.transaction(outputSchema, inputTaskCount, new ExecutorImpl(inputTaskCount, task, Collections.singletonList(DEFAULT_HOST)));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            control.transaction(outputSchema, inputTaskCount, new ExecutorImpl(inputTaskCount, task, task.getHosts()));
        }
    }

    private class ExecutorImpl implements ExecutorPlugin.Executor {
        private final PluginTask pluginTask;
        private final int inputTaskCount;
        private final List<Host> hosts;

        ExecutorImpl(int inputTaskCount, PluginTask pluginTask, List<Host> hosts) {
            this.inputTaskCount = inputTaskCount;
            this.pluginTask = pluginTask;
            this.hosts = hosts;
        }

        @Override
        public void execute(ProcessTask processTask, ProcessState state) {
            ModelManager modelManager = pluginTask.getModelManager();
            String systemConfigJson = modelManager.writeObject(systemConfig);
            String pluginTaskJson = modelManager.writeObject(pluginTask);
            String processTaskJson = modelManager.writeObject(processTask);

            SessionState sessionState = new SessionState(
                    systemConfigJson, pluginTaskJson, processTaskJson, state, inputTaskCount, modelManager);
            try (EmbulkClient client = EmbulkClient.open(sessionState, hosts)) {
                client.createSession();

                state.initialize(inputTaskCount, inputTaskCount);
                for (int i = 0; i < inputTaskCount; i++) {
                    if (state.getOutputTaskState(i).isCommitted()) {
                        log.warn("Skipped resumed task {}", i);
                        continue;
                    }
                    client.startTask(i);
                }
                sessionState.waitUntilCompleted(pluginTask.getTimeoutSeconds() + 1); // Add 1 sec to consider network latency
            } catch (InterruptedException | TimeoutException e) {
                throw new IllegalStateException(e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}

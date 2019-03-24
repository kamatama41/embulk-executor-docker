package org.embulk.executor.docker;

import com.google.inject.Inject;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.ModelManager;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.exec.ForSystemConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecutorPlugin;
import org.embulk.spi.ProcessState;
import org.embulk.spi.ProcessTask;
import org.embulk.spi.Schema;
import org.embulk.spi.util.Executors;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DockerExecutor implements ExecutorPlugin {
    private static final Logger LOGGER = LoggerFactory.getLogger(DockerExecutor.class);
    private final ConfigSource systemConfig;
    private final ScriptingContainer jruby;

    interface PluginTask extends Task {
        @ConfigInject
        ModelManager getModelManager();
    }

    @Inject
    public DockerExecutor(@ForSystemConfig ConfigSource systemConfig, ScriptingContainer jruby) {
        this.systemConfig = systemConfig;
        this.jruby = jruby;
    }

    @Override
    public void transaction(ConfigSource config, Schema outputSchema, int inputTaskCount, Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);
        control.transaction(outputSchema, inputTaskCount, new ExecutorImpl(inputTaskCount, task));
    }

    private class ExecutorImpl implements ExecutorPlugin.Executor {
        private final PluginTask pluginTask;
        private final int inputTaskCount;

        ExecutorImpl(int inputTaskCount, PluginTask pluginTask) {
            this.inputTaskCount = inputTaskCount;
            this.pluginTask = pluginTask;
        }

        @Override
        public void execute(ProcessTask processTask, ProcessState state) {
            ModelManager modelManager = pluginTask.getModelManager();
            String sysConfig = modelManager.writeObject(systemConfig);
            String pluginTaskJson = modelManager.writeObject(pluginTask);
            String processTaskJson = modelManager.writeObject(processTask);
            TaskRunner taskRunner = new TaskRunner(sysConfig, pluginTaskJson, processTaskJson);

            LOGGER.info("Start #execute");
            state.initialize(inputTaskCount, inputTaskCount);
            for (int i = 0; i < inputTaskCount; i++) {
                if (state.getOutputTaskState(i).isCommitted()) {
                    LOGGER.warn("Skipped resumed task {}", i);
                    continue;
                }
                taskRunner.run(i, state);
            }
        }
    }
}

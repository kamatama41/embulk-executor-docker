package org.embulk.executor.docker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import org.embulk.EmbulkEmbed;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.ModelManager;
import org.embulk.config.TaskReport;
import org.embulk.spi.ExecSession;
import org.embulk.spi.ProcessState;
import org.embulk.spi.ProcessTask;
import org.embulk.spi.util.Executors;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import static java.nio.charset.StandardCharsets.UTF_8;

public class TaskRunner {
    private final EmbulkEmbed embed;
    private final DockerExecutor.PluginTask pluginTask;
    private final ProcessTask processTask;
    private final ExecSession session;

    public TaskRunner(String systemConfig, String pluginTaskConfig, String processTaskConfig) {
        this.embed = newEmbulkBootstrap(systemConfig).initialize();
        this.pluginTask = getExecutorTask(embed.getInjector(), pluginTaskConfig);
        this.processTask = getProcessTask(embed.getInjector(), processTaskConfig);
        this.session = ExecSession.builder(embed.getInjector()).build();
    }

    public void run(int taskIndex, ProcessState state) {
        try {
            Executors.process(session, processTask, taskIndex, new Executors.ProcessStateCallback() {
                @Override
                public void started() {
                    state.getInputTaskState(taskIndex).start();
                    state.getOutputTaskState(taskIndex).start();
                }

                @Override
                public void inputCommitted(TaskReport report) {
                    state.getInputTaskState(taskIndex).setTaskReport(report);
                }

                @Override
                public void outputCommitted(TaskReport report) {
                    state.getOutputTaskState(taskIndex).setTaskReport(report);
                }
            });
        } finally {
            state.getInputTaskState(taskIndex).finish();
            state.getOutputTaskState(taskIndex).finish();
        }
    }

    private static EmbulkEmbed.Bootstrap newEmbulkBootstrap(String configJson) {
        ConfigSource systemConfig = getSystemConfig(configJson);
        return new EmbulkEmbed.Bootstrap().setSystemConfig(systemConfig);
    }

    private static DockerExecutor.PluginTask getExecutorTask(Injector injector, String configJson) {
        return injector.getInstance(ModelManager.class).readObject(DockerExecutor.PluginTask.class, configJson);
    }

    private static ProcessTask getProcessTask(Injector injector, String configJson) {
        return injector.getInstance(ModelManager.class).readObject(ProcessTask.class, configJson);
    }

    private static ConfigSource getSystemConfig(String configJson) {
        try {
            ModelManager bootstrapModelManager = new ModelManager(null, new ObjectMapper());
            try (InputStream in = new ByteArrayInputStream(configJson.getBytes(UTF_8))) {
                return new ConfigLoader(bootstrapModelManager).fromJson(in);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}

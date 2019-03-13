package org.embulk.executor.docker;

import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecutorPlugin;
import org.embulk.spi.ProcessState;
import org.embulk.spi.ProcessTask;
import org.embulk.spi.Schema;
import org.embulk.spi.util.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DockerExecutor implements ExecutorPlugin {
    private static final Logger LOGGER = LoggerFactory.getLogger(DockerExecutor.class);

    @Override
    public void transaction(ConfigSource config, Schema outputSchema, int inputTaskCount, Control control) {
        control.transaction(outputSchema, inputTaskCount, new ExecutorImpl(inputTaskCount));
    }

    private static class ExecutorImpl implements ExecutorPlugin.Executor {
        private final int inputTaskCount;

        ExecutorImpl(int inputTaskCount) {
            this.inputTaskCount = inputTaskCount;
        }

        @Override
        public void execute(ProcessTask task, ProcessState state) {
            LOGGER.info("Start #execute");
            state.initialize(inputTaskCount, inputTaskCount);

            for (int i = 0; i < inputTaskCount; i++) {
                if (state.getOutputTaskState(i).isCommitted()) {
                    LOGGER.warn("Skipped resumed task {}", i);
                    continue;
                }

                final int taskIndex = i;
                try {
                    Executors.process(Exec.session(), task, taskIndex, new Executors.ProcessStateCallback() {
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
        }
    }
}

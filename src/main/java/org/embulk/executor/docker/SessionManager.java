package org.embulk.executor.docker;

import org.embulk.config.ModelManager;
import org.embulk.config.TaskReport;
import org.embulk.spi.ProcessState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

class SessionManager {
    private static final Logger log = LoggerFactory.getLogger(SessionManager.class);
    private final ConcurrentMap<String, State> map;
    private final ModelManager modelManager;

    SessionManager(ModelManager modelManager) {
        this.map = new ConcurrentHashMap<>();
        this.modelManager = modelManager;
    }

    State registerNewSession(ProcessState state, int inputTaskCount) {
        State sessionState = new State(state, inputTaskCount);
        if (map.putIfAbsent(sessionState.getSessionId(), sessionState) != null) {
            throw new IllegalStateException();
        }
        return sessionState;
    }

    private void removeSession(State state) {
        map.remove(state.getSessionId(), state);
    }

    public State getState(String sessionId) {
        return map.get(sessionId);
    }

    class State {
        private final String sessionId;
        private final ProcessState state;
        private final CountDownLatch timer;

        State(ProcessState state, int inputTaskCount) {
            this.sessionId = UUID.randomUUID().toString();
            this.state = state;
            this.timer = new CountDownLatch(inputTaskCount);
        }

        String getSessionId() {
            return sessionId;
        }

        ProcessState getState() {
            return state;
        }

        synchronized void update(UpdateTaskStateData data) {
            switch (data.getTaskState()) {
                case STARTED:
                    log.warn("started: {}", data.getTaskIndex());
                    state.getInputTaskState(data.getTaskIndex()).start();
                    state.getOutputTaskState(data.getTaskIndex()).start();
                    break;
                case INPUT_COMMIITTED:
                    log.warn("input_committed: {}", data.getTaskIndex());
                    state.getInputTaskState(data.getTaskIndex()).setTaskReport(getTaskReport(data.getTaskReport()));
                    break;
                case OUTPUT_COMMITTED:
                    log.warn("output_committed: {}", data.getTaskIndex());
                    state.getOutputTaskState(data.getTaskIndex()).setTaskReport(getTaskReport(data.getTaskReport()));
                    break;
                case FINISHED:
                    log.warn("finished: {}", data.getTaskIndex());
                    state.getInputTaskState(data.getTaskIndex()).finish();
                    state.getOutputTaskState(data.getTaskIndex()).finish();
                    timer.countDown();
                    break;
            }
        }

        void waitUntilFinished() throws InterruptedException {
            try {
                timer.await();
            } finally {
                removeSession(this);
            }
        }
    }

    private TaskReport getTaskReport(String json) {
        return modelManager.readObject(TaskReport.class, json);
    }
}

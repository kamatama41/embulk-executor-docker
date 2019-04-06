package org.embulk.executor.remoteserver;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.kamatama41.nsocket.Command;
import com.github.kamatama41.nsocket.Connection;

class StartTaskCommand implements Command<StartTaskCommand.Data> {
    static final String ID = "start_task";
    private final ServerSessionRegistry sessionRegistry;

    StartTaskCommand(ServerSessionRegistry sessionRegistry) {
        this.sessionRegistry = sessionRegistry;
    }

    @Override
    public void execute(Data data, Connection connection) {
        ServerSession session = sessionRegistry.get(data.getSessionId());
        if (session == null) {
            throw new IllegalStateException("Session is not created.");
        }
        session.runTaskAsynchronously(data.getTaskIndex());
    }

    @Override
    public String getId() {
        return ID;
    }

    static class Data {
        private String sessionId;
        private int taskIndex;

        @JsonCreator
        Data(@JsonProperty("sessionId") String sessionId,
             @JsonProperty("taskIndex") int taskIndex) {
            this.sessionId = sessionId;
            this.taskIndex = taskIndex;
        }

        @JsonProperty
        String getSessionId() {
            return sessionId;
        }

        @JsonProperty
        int getTaskIndex() {
            return taskIndex;
        }
    }
}

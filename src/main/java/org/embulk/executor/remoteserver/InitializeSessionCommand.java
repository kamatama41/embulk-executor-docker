package org.embulk.executor.remoteserver;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.kamatama41.nsocket.Connection;
import com.github.kamatama41.nsocket.SyncCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InitializeSessionCommand implements SyncCommand<InitializeSessionCommand.Data, Void> {
    static final String ID = "initialize_session";
    private static final Logger log = LoggerFactory.getLogger(InitializeSessionCommand.class);
    private final SessionManager sessionManager;

    InitializeSessionCommand(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    @Override
    public Void apply(Data data, Connection connection) {
        sessionManager.registerNewSession(
                data.getSessionId(),
                data.getSystemConfigJson(),
                data.getPluginTaskJson(),
                data.getProcessTaskJson(),
                connection);
        return null;
    }

    @Override
    public long getTimeoutMillis() {
        return 60000L;
    }

    @Override
    public String getId() {
        return ID;
    }

    static class Data {
        private String sessionId;
        private String systemConfigJson;
        private String pluginTaskJson;
        private String processTaskJson;

        @JsonCreator
        Data(@JsonProperty("sessionId") String sessionId,
             @JsonProperty("systemConfigJson") String systemConfigJson,
             @JsonProperty("pluginTaskJson") String pluginTaskJson,
             @JsonProperty("processTaskJson") String processTaskJson) {
            this.sessionId = sessionId;
            this.systemConfigJson = systemConfigJson;
            this.pluginTaskJson = pluginTaskJson;
            this.processTaskJson = processTaskJson;
        }

        @JsonProperty
        String getSessionId() {
            return sessionId;
        }

        @JsonProperty
        String getSystemConfigJson() {
            return systemConfigJson;
        }

        @JsonProperty
        String getPluginTaskJson() {
            return pluginTaskJson;
        }

        @JsonProperty
        String getProcessTaskJson() {
            return processTaskJson;
        }
    }
}

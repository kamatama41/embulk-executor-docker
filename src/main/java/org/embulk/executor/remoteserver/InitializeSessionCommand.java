package org.embulk.executor.remoteserver;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.kamatama41.nsocket.Connection;
import com.github.kamatama41.nsocket.SyncCommand;

import java.util.List;

class InitializeSessionCommand implements SyncCommand<InitializeSessionCommand.Data, Void> {
    static final String ID = "initialize_session";
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
                data.getGemSpecs(),
                data.getPluginArchive(),
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
        private List<PluginArchive.GemSpec> gemSpecs;
        private byte[] pluginArchive;

        @JsonCreator
        Data(@JsonProperty("sessionId") String sessionId,
             @JsonProperty("systemConfigJson") String systemConfigJson,
             @JsonProperty("pluginTaskJson") String pluginTaskJson,
             @JsonProperty("processTaskJson") String processTaskJson,
             @JsonProperty("gemSpecs") List<PluginArchive.GemSpec>  gemSpecs,
             @JsonProperty("pluginArchive") byte[] pluginArchive) {
            this.sessionId = sessionId;
            this.systemConfigJson = systemConfigJson;
            this.pluginTaskJson = pluginTaskJson;
            this.processTaskJson = processTaskJson;
            this.gemSpecs = gemSpecs;
            this.pluginArchive = pluginArchive;
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

        @JsonProperty
        List<PluginArchive.GemSpec> getGemSpecs() {
            return gemSpecs;
        }

        @JsonProperty
        byte[] getPluginArchive() {
            return pluginArchive;
        }
    }
}

package org.embulk.executor.docker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.kamatama41.nsocket.Connection;
import com.github.kamatama41.nsocket.SyncCommand;

class CreateSessionCommand implements SyncCommand<CreateSessionCommand.Data, Void> {
    @Override
    public Void apply(CreateSessionCommand.Data data, Connection connection) throws Exception {
        Session session = new Session(data.getSessionId(), data.getSystemConfigJson(), data.getPluginTaskJson(), data.getProcessTaskJson());
        connection.attach(session);
        return null;
    }

    @Override
    public long getTimeoutMillis() {
        return 60000L;
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

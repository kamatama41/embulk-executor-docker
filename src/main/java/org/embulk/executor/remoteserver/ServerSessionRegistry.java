package org.embulk.executor.remoteserver;

import com.github.kamatama41.nsocket.Connection;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class ServerSessionRegistry {
    private final ConcurrentMap<String, ServerSession> sessionMap;

    ServerSessionRegistry() {
        this.sessionMap = new ConcurrentHashMap<>();
    }

    void register(String sessionId,
                  String systemConfig,
                  String pluginTaskConfig,
                  String processTaskConfig,
                  List<PluginArchive.GemSpec> gemSpecs,
                  byte[] pluginArchive,
                  Connection connection) {
        ServerSession session = sessionMap.computeIfAbsent(
                sessionId, (k) -> new ServerSession(
                        sessionId, systemConfig, pluginTaskConfig, processTaskConfig, gemSpecs, pluginArchive));
        session.updateConnection(connection);
    }

    ServerSession get(String sessionId) {
        return sessionMap.get(sessionId);
    }

    void remove(String sessionId) {
        ServerSession removed = sessionMap.remove(sessionId);
        removed.close();
    }
}

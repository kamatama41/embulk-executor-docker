package org.embulk.executor.remoteserver;

import com.github.kamatama41.nsocket.Connection;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class SessionManager {
    private final ConcurrentMap<String, Session> sessionMap;

    SessionManager() {
        this.sessionMap = new ConcurrentHashMap<>();
    }

    void registerNewSession(String sessionId,
                            String systemConfig,
                            String pluginTaskConfig,
                            String processTaskConfig,
                            List<PluginArchive.GemSpec> gemSpecs,
                            byte[] pluginArchive,
                            Connection connection) {
        Session session = sessionMap.computeIfAbsent(
                sessionId, (k) -> new Session(
                        sessionId, systemConfig, pluginTaskConfig, processTaskConfig, gemSpecs, pluginArchive));
        session.updateConnection(connection);
    }

    Session getSession(String sessionId) {
        return sessionMap.get(sessionId);
    }

    void removeSession(String sessionId) {
        Session removed = sessionMap.remove(sessionId);
        removed.close();
    }
}

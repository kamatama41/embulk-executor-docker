package org.embulk.executor.remoteserver;

import com.github.kamatama41.nsocket.CommandListener;
import com.github.kamatama41.nsocket.Connection;
import com.github.kamatama41.nsocket.SocketClient;
import org.embulk.config.ConfigException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

class EmbulkClient implements AutoCloseable {
    private final SocketClient client;
    private final List<Host> hosts;
    private final SessionState sessionState;
    private final AtomicInteger counter = new AtomicInteger(1);

    private EmbulkClient(SocketClient client, List<Host> hosts, SessionState sessionState) {
        this.client = client;
        this.hosts = hosts;
        this.sessionState = sessionState;
    }

    static EmbulkClient open(
            SessionState sessionState,
            List<Host> hosts) throws IOException {
        SocketClient client = new SocketClient();
        client.registerSyncCommand(new InitializeSessionCommand(null));
        client.registerSyncCommand(new RemoveSessionCommand(null));
        client.registerCommand(new NotifyTaskStateCommand(sessionState));
        client.registerListener(new Reconnector(client, sessionState));
        client.open();
        for (Host host : hosts) {
            client.addNode(host.toAddress());
        }
        return new EmbulkClient(client, hosts, sessionState);
    }

    void createSession() {
        ExecutorService es = Executors.newFixedThreadPool(hosts.size());
        List<Future> futures = new ArrayList<>();
        for (Host host : hosts) {
            futures.add(es.submit(() -> {
                Connection connection = client.getConnection(host.toAddress());
                connection.sendSyncCommand(InitializeSessionCommand.ID, toInitializeSessionData(sessionState));
            }));
        }
        try {
            for (Future future : futures) {
                future.get();
            }
        } catch (ExecutionException e) {
            throw new ConfigException(e.getCause());
        } catch (Exception e) {
            throw new ConfigException(e);
        } finally {
            es.shutdown();
        }
    }

    void startTask(int taskIndex) {
        // Round robin (more smart logic needed?)
        InetSocketAddress target = hosts.get(counter.getAndIncrement() % hosts.size()).toAddress();
        client.getConnection(target).sendCommand(
                StartTaskCommand.ID, new StartTaskCommand.Data(sessionState.getSessionId(), taskIndex));
    }

    @Override
    public void close() throws IOException {
        for (Host host : hosts) {
            Connection connection = client.getConnection(host.toAddress());
            connection.sendSyncCommand(RemoveSessionCommand.ID, sessionState.getSessionId());
        }
        client.close();
    }

    private static class Reconnector implements CommandListener {
        private final SocketClient client;
        private final SessionState sessionState;

        Reconnector(SocketClient client, SessionState sessionState) {
            this.client = client;
            this.sessionState = sessionState;
        }

        @Override
        public void onDisconnected(Connection connection) {
            if(!sessionState.isFinished()) {
                Connection newConnection = client.getConnection((InetSocketAddress) connection.getRemoteSocketAddress());
                newConnection.sendSyncCommand(InitializeSessionCommand.ID, toInitializeSessionData(sessionState));
            }
        }
    }

    private static InitializeSessionCommand.Data toInitializeSessionData(SessionState sessionState) {
        return new InitializeSessionCommand.Data(
                sessionState.getSessionId(),
                sessionState.getSystemConfigJson(),
                sessionState.getPluginTaskJson(),
                sessionState.getProcessTaskJson(),
                sessionState.getPluginArchiveBytes()
        );
    }
}

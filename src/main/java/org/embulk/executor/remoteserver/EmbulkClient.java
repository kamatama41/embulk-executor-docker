package org.embulk.executor.remoteserver;

import com.github.kamatama41.nsocket.CommandListener;
import com.github.kamatama41.nsocket.Connection;
import com.github.kamatama41.nsocket.SocketClient;
import org.embulk.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

class EmbulkClient extends SocketClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(EmbulkClient.class);
    private final ClientSession session;
    private final AtomicInteger counter;

    private EmbulkClient(ClientSession session) {
        super();
        this.session = session;
        this.counter = new AtomicInteger(0);
    }

    static EmbulkClient open(
            ClientSession session,
            List<Host> hosts) throws IOException {
        EmbulkClient client = new EmbulkClient(session);
        client.open();

        for (Host host : hosts) {
            try {
                client.addNode(host.toAddress());
            } catch (IOException e) {
                log.warn(String.format("Failed to connect to %s", host.toString()), e);
            }
        }

        if (client.getActiveConnections().isEmpty()) {
            throw new IOException("Failed to connect all hosts");
        }
        return client;
    }

    @Override
    public synchronized void open() throws IOException {
        registerSyncCommand(new InitializeSessionCommand(null));
        registerSyncCommand(new RemoveSessionCommand(null));
        registerCommand(new UpdateTaskStateCommand(session));
        registerListener(new Reconnector());
        super.open();
    }

    void createSession() {
        List<Connection> activeConnections = getActiveConnections();
        ExecutorService es = Executors.newFixedThreadPool(activeConnections.size());
        List<Future> futures = new ArrayList<>();
        for (Connection connection : activeConnections) {
            futures.add(es.submit(() -> {
                connection.sendSyncCommand(InitializeSessionCommand.ID, toInitializeSessionData(session));
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
        List<Connection> activeConnections = getActiveConnections();
        Connection target = activeConnections.get(counter.getAndIncrement() % activeConnections.size());
        target.sendCommand(StartTaskCommand.ID, new StartTaskCommand.Data(session.getId(), taskIndex));
    }

    @Override
    public void close() throws IOException {
        for (Connection connection : getActiveConnections()) {
            connection.sendSyncCommand(RemoveSessionCommand.ID, session.getId());
        }
        session.close();
        super.close();
    }

    private class Reconnector implements CommandListener {
        @Override
        public void onDisconnected(Connection connection) {
            if(!session.isFinished()) {
                try {
                    // Try reconnecting
                    Connection newConnection = reconnect(connection);
                    newConnection.sendSyncCommand(InitializeSessionCommand.ID, toInitializeSessionData(session));
                } catch (IOException e) {
                    log.warn(String.format("A connection to %s could not be reconnected.", connection.getRemoteSocketAddress()), e);
                }
            }
        }
    }

    private static InitializeSessionCommand.Data toInitializeSessionData(ClientSession session) {
        return new InitializeSessionCommand.Data(
                session.getId(),
                session.getSystemConfigJson(),
                session.getPluginTaskJson(),
                session.getProcessTaskJson(),
                session.getGemSpecs(),
                session.getPluginArchiveBytes()
        );
    }
}

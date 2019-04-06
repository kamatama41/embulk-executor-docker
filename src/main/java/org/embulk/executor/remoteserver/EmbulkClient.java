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
    private final ClientSession session;
    private final AtomicInteger counter = new AtomicInteger(1);

    private EmbulkClient(SocketClient client, List<Host> hosts, ClientSession session) {
        this.client = client;
        this.hosts = hosts;
        this.session = session;
    }

    static EmbulkClient open(
            ClientSession session,
            List<Host> hosts) throws IOException {
        SocketClient client = new SocketClient();
        client.registerSyncCommand(new InitializeSessionCommand(null));
        client.registerSyncCommand(new RemoveSessionCommand(null));
        client.registerCommand(new UpdateTaskStateCommand(session));
        client.registerListener(new Reconnector(client, session));
        client.open();
        for (Host host : hosts) {
            client.addNode(host.toAddress());
        }
        return new EmbulkClient(client, hosts, session);
    }

    void createSession() {
        ExecutorService es = Executors.newFixedThreadPool(hosts.size());
        List<Future> futures = new ArrayList<>();
        for (Host host : hosts) {
            futures.add(es.submit(() -> {
                Connection connection = client.getConnection(host.toAddress());
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
        InetSocketAddress target = hosts.get(counter.getAndIncrement() % hosts.size()).toAddress();
        client.getConnection(target).sendCommand(
                StartTaskCommand.ID, new StartTaskCommand.Data(session.getId(), taskIndex));
    }

    @Override
    public void close() throws IOException {
        for (Host host : hosts) {
            Connection connection = client.getConnection(host.toAddress());
            connection.sendSyncCommand(RemoveSessionCommand.ID, session.getId());
        }
        client.close();
    }

    private static class Reconnector implements CommandListener {
        private final SocketClient client;
        private final ClientSession session;

        Reconnector(SocketClient client, ClientSession session) {
            this.client = client;
            this.session = session;
        }

        @Override
        public void onDisconnected(Connection connection) {
            if(!session.isFinished()) {
                Connection newConnection = client.getConnection((InetSocketAddress) connection.getRemoteSocketAddress());
                newConnection.sendSyncCommand(InitializeSessionCommand.ID, toInitializeSessionData(session));
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

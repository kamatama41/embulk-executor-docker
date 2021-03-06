package org.embulk.executor.remoteserver;

import com.google.inject.Inject;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.ModelManager;
import org.embulk.config.Task;
import org.embulk.exec.ForSystemConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecutorPlugin;
import org.embulk.spi.ProcessState;
import org.embulk.spi.ProcessTask;
import org.embulk.spi.Schema;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class RemoteServerExecutor implements ExecutorPlugin {
    private static final Logger log = LoggerFactory.getLogger(RemoteServerExecutor.class);
    private static final Host DEFAULT_HOST = new Host("localhost", 30001);
    private final ConfigSource systemConfig;
    private final ScriptingContainer jruby;

    interface PluginTask extends Task {
        @Config("hosts")
        @ConfigDefault("[]")
        List<String> getHosts();

        @Config("timeout_seconds")
        @ConfigDefault("3600")
        int getTimeoutSeconds();

        @Config("use_tls")
        @ConfigDefault("false")
        boolean getUseTls();

        @Config("cert_p12_file")
        @ConfigDefault("null")
        Optional<P12File> getCertP12File();

        @Config("ca_cert_path")
        @ConfigDefault("null")
        Optional<String> getCaCertPath();

        @ConfigInject
        ModelManager getModelManager();

        // Used for the local mode (mainly for testing)
        @Config("__server_cert_p12_file")
        @ConfigDefault("null")
        Optional<P12File> getServerCertP12File();

        @Config("__server_ca_cert_path")
        @ConfigDefault("null")
        Optional<String> getServerCaCertPath();

        @Config("__server_require_tls_client_auth")
        @ConfigDefault("false")
        boolean getServerRequireTlsClientAuth();
    }

    @Inject
    public RemoteServerExecutor(@ForSystemConfig ConfigSource systemConfig, ScriptingContainer jruby) {
        this.systemConfig = systemConfig;
        this.jruby = jruby;
    }

    @Override
    public void transaction(ConfigSource config, Schema outputSchema, int inputTaskCount, Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);
        if (task.getHosts().isEmpty()) {
            log.info("Hosts is empty. Run as the local mode.");
            try (EmbulkServer _autoclosed = startEmbulkServer(task)) {
                control.transaction(outputSchema, inputTaskCount, new ExecutorImpl(inputTaskCount, task, Collections.singletonList(DEFAULT_HOST)));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            control.transaction(
                    outputSchema,
                    inputTaskCount,
                    new ExecutorImpl(inputTaskCount, task, task.getHosts().stream().map(Host::of).collect(Collectors.toList())));
        }
    }

    private class ExecutorImpl implements ExecutorPlugin.Executor {
        private final PluginTask pluginTask;
        private final int inputTaskCount;
        private final List<Host> hosts;

        ExecutorImpl(int inputTaskCount, PluginTask pluginTask, List<Host> hosts) {
            this.inputTaskCount = inputTaskCount;
            this.pluginTask = pluginTask;
            this.hosts = hosts;
        }

        @Override
        public void execute(ProcessTask processTask, ProcessState state) {
            byte[] pluginArchiveBytes;
            List<PluginArchive.GemSpec> gemSpecs;
            try {
                File tempFile = Exec.getTempFileSpace().createTempFile("gems", ".zip");
                gemSpecs = archivePlugins(tempFile);
                pluginArchiveBytes = Files.readAllBytes(tempFile.toPath());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            // Remove 'jruby_global_bundler_plugin_source_directory' (--bundle option)
            // because all gems will be loaded via PluginArchive on server
            ConfigSource systemConfigToSend = systemConfig.deepCopy().remove("jruby_global_bundler_plugin_source_directory");

            ModelManager modelManager = pluginTask.getModelManager();
            String systemConfigJson = modelManager.writeObject(systemConfigToSend);
            String pluginTaskJson = modelManager.writeObject(pluginTask);
            String processTaskJson = modelManager.writeObject(processTask);

            ClientSession session = new ClientSession(
                    systemConfigJson, pluginTaskJson, processTaskJson, gemSpecs, pluginArchiveBytes, state, inputTaskCount, modelManager);

            TLSConfig tlsConfig = null;
            if (pluginTask.getUseTls()) {
                tlsConfig = new TLSConfig();
                pluginTask.getCertP12File().ifPresent(tlsConfig::setKeyStore);
                pluginTask.getCaCertPath().ifPresent(tlsConfig::setCaCertPath);
            }

            try (EmbulkClient client = EmbulkClient.open(session, hosts, tlsConfig)) {
                client.createSession();

                state.initialize(inputTaskCount, inputTaskCount);
                for (int i = 0; i < inputTaskCount; i++) {
                    if (state.getOutputTaskState(i).isCommitted()) {
                        log.warn("Skipped resumed task {}", i);
                        continue;
                    }
                    client.startTask(i);
                }
                session.waitUntilCompleted(pluginTask.getTimeoutSeconds() + 1); // Add 1 sec to consider network latency
            } catch (InterruptedException | TimeoutException e) {
                throw new IllegalStateException(e);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private List<PluginArchive.GemSpec> archivePlugins(File tempFile) throws IOException {
            // archive plugins
            PluginArchive archive = new PluginArchive.Builder()
                    .addLoadedRubyGems(jruby)
                    .build();
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                return archive.dump(fos);
            }
        }
    }

    private EmbulkServer startEmbulkServer(PluginTask task) throws IOException {
        TLSConfig tlsConfig = null;
        if (task.getUseTls()) {
            tlsConfig = new TLSConfig();
            task.getServerCertP12File().ifPresent(tlsConfig::setKeyStore);
            task.getServerCaCertPath().ifPresent(tlsConfig::setCaCertPath);
            tlsConfig.setEnableClientAuth(task.getServerRequireTlsClientAuth());
        }
        return EmbulkServer.start(DEFAULT_HOST.getName(), DEFAULT_HOST.getPort(), 1, tlsConfig);
    }
}

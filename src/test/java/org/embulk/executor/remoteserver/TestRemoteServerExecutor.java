package org.embulk.executor.remoteserver;

import org.embulk.config.ConfigSource;
import org.embulk.test.EmbulkPluginTest;
import org.embulk.test.EmbulkTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@EmbulkTest(value = RemoteServerExecutor.class, name = "remoteserver")
class TestRemoteServerExecutor extends EmbulkPluginTest {
    private static final List<Host> HOSTS = Arrays.asList(new Host("localhost", 24224), new Host("localhost", 24225));
    private static final List<EmbulkServer> SERVERS = new ArrayList<>();
    private static final Path TEMP_DIR;
    static {
        try {
            TEMP_DIR = Files.createTempDirectory("file_output");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @BeforeAll
    static void startServers() throws IOException {
        for (Host host : HOSTS) {
            SERVERS.add(EmbulkServer.start(host.getName(), host.getPort(), 1));
        }
    }

    @AfterAll
    static void stopServers() throws IOException {
        for (EmbulkServer server : SERVERS) {
            server.close();
        }
    }

    @Test
    void testSimpleCase() {
        ConfigSource inConfig = config().set("type", "file")
                .set("path_prefix", "src/test/resources/json/test")
                .set("parser", config().set("type", "json"));

        ConfigSource execConfig = config().set("type", "remoteserver")
                .set("hosts", HOSTS);

        Path pathPrefix = TEMP_DIR.resolve("out_");
        ConfigSource outConfig = config().set("type", "file")
                .set("path_prefix", pathPrefix.toFile().getAbsolutePath())
                .set("file_ext", "json")
                .set("formatter", config()
                        .set("type", "csv")
                        .set("header_line", false)
                        .set("quote_policy", "NONE")
                );

        runExec(inConfig, execConfig, outConfig);
        File[] files = TEMP_DIR.toFile().listFiles();
        assertEquals(files.length, 2);
        List<String> outputs = Arrays.stream(files).map(f -> {
            try {
                return String.join("", Files.readAllLines(f.toPath()));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).sorted().collect(Collectors.toList());

        List<String> expected = Arrays.asList("{\"a\":1}", "{\"a\":2}");
        assertEquals(expected, outputs);
    }
}

package org.embulk.executor.remoteserver;

import org.embulk.config.ConfigSource;
import org.embulk.test.EmbulkPluginTest;
import org.embulk.test.EmbulkTest;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@EmbulkTest(value = RemoteServerExecutor.class, name = "remoteserver")
class TestRemoteServerExecutor extends EmbulkPluginTest {
    private static final List<Host> HOSTS = Arrays.asList(new Host("localhost", 24224), new Host("localhost", 24225));
    private static final Path OUTPUT_DIR = Paths.get("tmp", "output");
    private static final Path TEST_DIR = Paths.get("test");

    @Test
    void testSimpleCase() {
        setSystemConfig(config().set("jruby_global_bundler_plugin_source_directory", TEST_DIR.toFile().getAbsolutePath()));

        ConfigSource inConfig = config().set("type", "file")
                .set("path_prefix", "src/test/resources/json/test")
                .set("parser", config().set("type", "json")
                        .set("columns", Collections.singletonList(config()
                                .set("name", "a").set("type", "long")
                        ))
                );

        ConfigSource execConfig = config().set("type", "remoteserver")
                .set("hosts", HOSTS)
                .set("timeout_seconds", 5);

        ConfigSource filterConfig = config().set("type", "hash")
                .set("columns", Collections.singletonList(config()
                        .set("name", "a").set("algorithm", "MD5")
                ));

        ConfigSource outConfig = config().set("type", "file")
                .set("path_prefix", "/output/out_file_")
                .set("file_ext", "json")
                .set("formatter", config()
                        .set("type", "csv")
                        .set("header_line", false)
                        .set("quote_policy", "NONE")
                );

        runConfig(inConfig)
                .execConfig(execConfig)
                .filterConfig(filterConfig)
                .outConfig(outConfig).run();

        File[] files = OUTPUT_DIR.toFile().listFiles();
        assertEquals(2, files.length);
        Set<String> outputs = Arrays.stream(files).map(f -> {
            try {
                return String.join("", Files.readAllLines(f.toPath()));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).collect(Collectors.toSet());

        Set<String> expected = new HashSet<String>(){{
            add("c4ca4238a0b923820dcc509a6f75849b"); // "1" of MD5
            add("c81e728d9d4c2f636f067f89cc14862c"); // "2" of MD5
        }};
        assertEquals(expected, outputs);
    }
}

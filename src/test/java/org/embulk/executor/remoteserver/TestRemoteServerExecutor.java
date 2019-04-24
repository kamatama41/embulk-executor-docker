package org.embulk.executor.remoteserver;

import org.embulk.config.ConfigSource;
import org.embulk.test.EmbulkPluginTest;
import org.embulk.test.EmbulkTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.embulk.test.Utils.configFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;

@EmbulkTest(value = RemoteServerExecutor.class, name = "remoteserver")
class TestRemoteServerExecutor extends EmbulkPluginTest {
    private static final Path OUTPUT_DIR = Paths.get("tmp", "output");
    private static final Path TEST_DIR = Paths.get("test");

    @BeforeEach
    void cleanupOutputDir() {
        File outputDir = OUTPUT_DIR.toFile();
        if (outputDir.exists()) {
            Arrays.stream(outputDir.listFiles()).forEach(File::delete);
        }
    }

    @Test
    void testSimpleCase() {
        setSystemConfig(config().set("jruby_global_bundler_plugin_source_directory", TEST_DIR.toFile().getAbsolutePath()));

        ConfigSource inConfig = configFromResource("config/input.yml");
        ConfigSource execConfig = configFromResource("config/exec_base.yml");
        ConfigSource filterConfig = configFromResource("config/filter.yml");
        ConfigSource outConfig = configFromResource("config/output.yml");

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

    @Test
    void testConnectWithTLS() {
        setSystemConfig(config().set("jruby_global_bundler_plugin_source_directory", TEST_DIR.toFile().getAbsolutePath()));

        ConfigSource inConfig = configFromResource("config/input.yml");
        ConfigSource execConfig = configFromResource("config/exec_tls.yml");
        ConfigSource filterConfig = configFromResource("config/filter.yml");
        ConfigSource outConfig = configFromResource("config/output.yml");

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

package net.tutysara.db;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DiskStoreTest {

    private static final String filename = "test.db";
    private static final Logger log = LoggerFactory.getLogger(DiskStoreTest.class);

    private void deleteFile() throws IOException {
        Path file = Paths.get(DiskStoreTest.filename);
        Files.deleteIfExists(file);
    }

    @BeforeEach
    void setUp() throws IOException {
        deleteFile();
    }

    @AfterEach
    void tearDown() throws IOException {
        //deleteFile(filename);
    }

    @Test
    void test_Get() {
        try (DiskStore store = new DiskStore(filename)) {
            store.set("name", "jojo");
            var value = store.get("name");
            assertEquals("jojo", value);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    @Test
    void test_GetInvalid() {
        try (DiskStore store = new DiskStore(filename)) {
            var value = store.get("somekey");
            assertEquals(0, value.length());
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    @Test
    void test_SetWithPersistence() throws Exception {
        var tests = Map.of("name", "jojo",
                "crime and punishment", "dostoevsky",
                "anna karenina", "tolstoy",
                "war and peace", "tolstoy",
                "hamlet", "shakespeare",
                "othello", "shakespeare",
                "brave new world", "huxley",
                "dune", "frank herbert");

        try (DiskStore ds = new DiskStore(filename)) {

            tests.forEach((key, value) -> {
                try {
                    ds.set(key, value);
                    var readValue = ds.get(key);
                    assertEquals(value, readValue);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        // read from existing file
        try (DiskStore ds = new DiskStore(filename)) {
            tests.forEach((key, value) -> {
                try {
                    var readValue = ds.get(key);
                    assertEquals(value, readValue);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Test
    void test_Delete() throws Exception {
        var tests = Map.of("name", "jojo",
                "crime and punishment", "dostoevsky",
                "anna karenina", "tolstoy",
                "war and peace", "tolstoy",
                "hamlet", "shakespeare",
                "othello", "shakespeare",
                "brave new world", "huxley",
                "dune", "frank herbert");

        try (DiskStore ds = new DiskStore(filename)) {

            tests.forEach((key, value) -> {
                try {
                    ds.set(key, value);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            tests.forEach((key, value) -> {
                try {
                    ds.set(key, "");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        // read from existing file and verify deletes
        try (DiskStore ds = new DiskStore(filename)) {
            tests.forEach((key, value) -> {
                try {
                    var readValue = ds.get(key);
                    assertEquals("", readValue);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
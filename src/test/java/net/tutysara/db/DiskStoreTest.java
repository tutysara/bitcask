package net.tutysara.db;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DiskStoreTest {

    private static final Path TEST_DB = Path.of("test.db");

    @AfterEach
    void cleanup() throws IOException {
        Files.deleteIfExists(TEST_DB);
    }

    @Test
    void testDiskStore_Get() throws IOException {
        DiskStore store = new DiskStore("test.db");
        store.set("name", "jojo");
        assertEquals("jojo", store.get("name"));
    }

    @Test
    void testDiskStore_GetInvalid() throws IOException {
        DiskStore store = new DiskStore("test.db");
        assertEquals("", store.get("some key"));
    }

    @Test
    void testDiskStore_SetWithPersistence() throws IOException {
        Map<String, String> tests = Map.of(
                "crime and punishment", "dostoevsky",
                "anna karenina", "tolstoy",
                "war and peace", "tolstoy",
                "hamlet", "shakespeare",
                "othello", "shakespeare",
                "brave new world", "huxley",
                "dune", "frank herbert"
        );

        DiskStore store = new DiskStore("test.db");
        for (var entry : tests.entrySet()) {
            store.set(entry.getKey(), entry.getValue());
            assertEquals(entry.getValue(), store.get(entry.getKey()));
        }
        store.close();

        store = new DiskStore("test.db");
        for (var entry : tests.entrySet()) {
            assertEquals(entry.getValue(), store.get(entry.getKey()));
        }
        store.close();
    }

    @Test
    void testDiskStore_Delete() throws IOException {
        Map<String, String> tests = Map.of(
                "crime and punishment", "dostoevsky",
                "anna karenina", "tolstoy",
                "war and peace", "tolstoy",
                "hamlet", "shakespeare",
                "othello", "shakespeare",
                "brave new world", "huxley",
                "dune", "frank herbert"
        );

        DiskStore store = new DiskStore("test.db");
        for (var entry : tests.entrySet()) {
            store.set(entry.getKey(), entry.getValue());
        }
        for (var key : tests.keySet()) {
            store.set(key, ""); // simulate delete
        }
        store.set("end", "yes");
        store.close();

        store = new DiskStore("test.db");
        for (var key : tests.keySet()) {
            assertEquals("", store.get(key));
        }
        assertEquals("yes", store.get("end"));
        store.close();
    }

}
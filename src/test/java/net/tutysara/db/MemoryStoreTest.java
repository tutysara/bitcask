package net.tutysara.db;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MemoryStoreTest {
    @Test
    void testMemoryStoreGet() {
        MemoryStore store = new MemoryStore();
        store.set("name", "jojo");
        assertEquals("jojo", store.get("name"), "Get() should return 'jojo'");
    }

    @Test
    void testMemoryStoreInvalidGet() {
        MemoryStore store = new MemoryStore();
        assertEquals("", store.get("some rando key"),
                "Get() on unknown key should return empty string");
    }

    @Test
    void testMemoryStoreClose() {
        MemoryStore store = new MemoryStore();
        assertTrue(store.close(), "Close() should return true");
    }

}
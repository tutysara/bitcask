package net.tutysara.db;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryStore implements Store{
    private final Map<String, String> memStore = new ConcurrentHashMap<>();
    @Override
    public void set(String key, String value) {
        memStore.put(key, value);
    }

    @Override
    public String get(String key) {
        return memStore.get(key);
    }

    @Override
    public Set<String> listKeys() {
        return memStore.keySet();
    }

    @Override
    public boolean close() {
        memStore.clear();
        return true;
    }
}

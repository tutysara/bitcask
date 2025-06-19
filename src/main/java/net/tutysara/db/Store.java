package net.tutysara.db;

import java.io.IOException;
import java.util.Set;

public interface Store {
    void set(String key, String value);
    String get(String key);
    Set<String> listKeys();
    boolean close();
}

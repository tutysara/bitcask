package net.tutysara.db;

import java.io.IOException;

public interface Store {
    public String get(String key) throws IOException;
    public void set(String key, String value) throws IOException;
    public void close() throws IOException;
}

package net.tutysara.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class App 
{
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    public static void main( String[] args ) throws IOException {
        try(DiskStore ds = new DiskStore("test.db")) {
            ds.set("name", "jojo");
            var name = ds.get("name");
            System.out.println(name);
            assert name == "jojo";
        } catch (Exception e) {
            logger.error("Error while closing datastore", e);
        }
    }
}

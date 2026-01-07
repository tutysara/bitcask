package net.tutysara.db;

import java.io.IOException;


public class App 
{
    public static void main( String[] args ) throws IOException {
        DiskStore ds = new DiskStore("test.db");
        ds.set("name", "jojo");
        var name = ds.get("name");
        System.out.println(name);
        assert name == "jojo";
    }
}

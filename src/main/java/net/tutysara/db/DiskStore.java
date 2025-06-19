package net.tutysara.db;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

record KeyEntry(long timestamp, long position, int totalSize) {
}

public class DiskStore implements Store {

    private final Map<String, KeyEntry> keyDir = new ConcurrentHashMap<>();
    private FileChannel fileChannel;
    private long writePosition;

    public DiskStore(String filename) throws IOException {
        Path path = Paths.get(filename);

        var fileOptions = EnumSet.of(StandardOpenOption.WRITE,
                StandardOpenOption.READ);

        if (Files.exists(path)) {
            this.fileChannel = FileChannel.open(path, fileOptions);
            System.out.println("FileChannel opened for: " + path.toAbsolutePath());
            initKeyDir(fileChannel);
            this.writePosition = this.fileChannel.position();

        } else {
            fileOptions.add(StandardOpenOption.CREATE);
            this.fileChannel = FileChannel.open(path, fileOptions);
            System.out.println("FileChannel created for: " + path.toAbsolutePath());
            this.writePosition = 0;
        }
    }

    private synchronized void initKeyDir(FileChannel channel) throws IOException {
        // we will initialise the keyDir by reading the contents of the file, record by
        // record. As we read each record, we will also update our keyDir with the
        // corresponding KeyEntry
        //
        // NOTE: this method is a blocking one, if the DB size is yuge then it will take
        // a lot of time to startup
        System.out.println("Size: " + channel.size() + " bytes");
        while(this.fileChannel.position() < this.fileChannel.size()){
            var headerBuf = ByteBuffer.allocate(Format.HEADER_SIZE);
            this.fileChannel.read(headerBuf);
            Format.Header header = Format.decodeHeader(headerBuf.array());
            var timeStamp = header.timestamp();
            var totalSize = Format.HEADER_SIZE+ header.keySize() + header.valSize();
            // read key
            var keyBuf = ByteBuffer.allocate((header.keySize()));
            this.fileChannel.read(keyBuf);
            // read value to just move the position
            var valBuf = ByteBuffer.allocate((header.valSize()));
            this.fileChannel.read(valBuf);

            String key = new String(keyBuf.array(), StandardCharsets.UTF_8);
            String value = new String(valBuf.array(), StandardCharsets.UTF_8);

            this.keyDir.put(key, new KeyEntry(timeStamp, this.writePosition, totalSize));
            this.writePosition += totalSize;

            System.out.println(String.format("Channel pos=%d, key=%s, value=%s",
                    fileChannel.position(), key, value));
        }
    }

    @Override
    public synchronized void set(String key, String value) {
        // Set stores the key and value on the disk
        //
        // The steps to save a KV to disk is simple:
        // 1. Encode the KV into bytes
        // 2. Write the bytes to disk by appending to the file

        var timeStamp = System.currentTimeMillis();
        byte[] data = Format.encodeKV(timeStamp, key, value);
        try {
            this.fileChannel.position(this.writePosition);
            this.fileChannel.write(ByteBuffer.wrap(data));
            this.fileChannel.force(true); // sync
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        keyDir.put(key, new KeyEntry(timeStamp, this.writePosition, data.length));
        this.writePosition += data.length;
    }

    @Override
    public synchronized String get(String key) {
        KeyEntry entry = keyDir.get(key);
        if (entry == null) {
            return "";
        }

        ByteBuffer data = ByteBuffer.allocate(entry.totalSize());
        data.order(ByteOrder.LITTLE_ENDIAN);
        try {
            this.fileChannel.position(entry.position());
            this.fileChannel.read(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Format.KV res = Format.decodeKV(data.array());
        return res.value();
    }

    @Override
    public Set<String> listKeys() {
        return keyDir.keySet();
    }

    @Override
    public synchronized boolean close() {
        boolean res = true;
        try {
            this.fileChannel.force(true); // sync
            this.fileChannel.close();
        } catch (IOException e) {
            res = false;
        }
        return res;
    }
}

package net.tutysara.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

public class DiskStore implements Store{
    private FileChannel filechannel;
    private Map<String, Format.KeyEntry> keyDir =  new HashMap<>();

    private DiskStore(){
    }

    public DiskStore(String fileName) throws IOException {
        Path path = Paths.get(fileName);
        var filechannel = FileChannel.open(path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.SYNC);

        this.filechannel = filechannel;

        if(filechannel.size() > 0){
            initKeyDir();
        }
    }

    public void close() throws IOException {
        if(filechannel != null){
            filechannel.close();
        }
    }

    public String get(String key) throws IOException {
        // Get retrieves the value from the disk and returns. If the key does not
        // exist then it returns an empty string
        //
        // How get works?
        //	1. Check if there is any KeyEntry record for the key in keyDir
        //	2. Return an empty string if key doesn't exist
        //	3. If it exists, then read KeyEntry.totalSize bytes starting from the
        //     KeyEntry.position from the disk
        //	4. Decode the bytes into valid KV pair and return the value
        //

        var keyEntry = keyDir.get(key);
        if(keyEntry == null){
            return "";
        }
        // move the current pointer to the right offset
        filechannel.position(keyEntry.position());
        ByteBuffer buffer = ByteBuffer.allocate(keyEntry.totalSize());
        filechannel.read(buffer);
        buffer.flip();
        Format.DecoderResponse response = Format.decodeKV(buffer.array());
        return response.value();

    }

    public void set(String key, String value) throws IOException {
        // Set stores the key and value on the disk
        //
        // The steps to save a KV to disk is simple:
        // 1. Encode the KV into bytes
        // 2. Write the bytes to disk by appending to the file
        // 3. Update KeyDir with the KeyEntry of this key
        long timeStamp = System.currentTimeMillis() / 1000L;
        byte[] bytes = Format.encodeKV(timeStamp,  key, value);
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        // move to end of file and write
        long position = filechannel.size();
        int bytesWritten = filechannel.write(buffer, position);
        assert bytesWritten == bytes.length : " Mismatch between bytesWritten and bytes.length";
        keyDir.put(key, new Format.KeyEntry(timeStamp, position, bytesWritten));

    }

    private void initKeyDir() throws IOException {
        // we will initialise the keyDir by reading the contents of the file, record by
        // record. As we read each record, we will also update our keyDir with the
        // corresponding KeyEntry
        //
        // NOTE: this method is a blocking one, if the DB size is huge then it will take
        // a lot of time to startup

        System.out.println("Loading file data...");
        while(filechannel.position() < filechannel.size()){
            ByteBuffer buffer = ByteBuffer.allocate(Format.HEADER_SIZE);
            long position = filechannel.position();
            int bytesRead = filechannel.read(buffer);
            if(bytesRead == -1){
                break; // we have reached EOF
            }
            buffer.flip();
            var header = Format.decodeHeader(buffer.array());
            buffer.clear();

            ByteBuffer bufferKey = ByteBuffer.allocate(header.keySize());
            bytesRead = filechannel.read(bufferKey);
            assert bytesRead == header.keySize() : "Mismatch between bytesRead and header.keySize";
            bufferKey.flip();
            String key = new String(bufferKey.array());
            bufferKey.clear();

            ByteBuffer bufferValue = ByteBuffer.allocate(header.valueSize());
            bytesRead = filechannel.read(bufferValue);
            assert bytesRead == header.valueSize() : "Mismatch between bytesRead and header.valueSize";
            bufferValue.flip();
            String value = new String(bufferValue.array());
            bufferValue.clear();

            int totalSize = Format.HEADER_SIZE +  header.keySize() + header.valueSize();
            keyDir.put(key, new Format.KeyEntry(header.timeStamp().toLong(), position, totalSize));
            System.out.printf("loaded key=%s, value=%s\n", key, value);

        }
        System.out.println("Loaded " + keyDir.size() + " keys");
    }
}

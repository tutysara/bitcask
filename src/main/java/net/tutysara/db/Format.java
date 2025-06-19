package net.tutysara.db;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

// headerSize specifies the total header size. Our key value pair, when stored on disk
// looks like this:
//
//	┌───────────┬──────────┬────────────┬─────┬───────┐
//	│ timestamp │ key_size │ value_size │ key │ value │
//	└───────────┴──────────┴────────────┴─────┴───────┘
//
// This is analogous to a typical database's row (or a record). The total length of
// the row is variable, depending on the contents of the key and value.
//
// The first three fields form the header:
//
//	┌───────────────┬──────────────┬────────────────┐
//	│ timestamp(4B) │ key_size(4B) │ value_size(4B) │
//	└───────────────┴──────────────┴────────────────┘
//
// These three fields store unsigned integers of size 4 bytes, giving our header a
// fixed length of 12 bytes. Timestamp field stores the time the record we
// inserted in unix epoch seconds. Key size and value size fields store the length of
// bytes occupied by the key and value. The maximum integer
// stored by 4 bytes is 4,294,967,295 (2 ** 32 - 1), roughly ~4.2GB. So, the size of
// each key or value cannot exceed this. Theoretically, a single row can be as large
// as ~8.4GB.



final class Format {
    // constants
    //java Long is 8 bytes independent of platform so, header is of size 24
    public static final int HEADER_SIZE = 8 + 4 + 4;
    // Header metadata
    public record Header(long timestamp, int keySize, int valSize) {}
    // Decoded KV triple
    public record KV(long timestamp, String key, String value) {}

    public static byte[] encodeHeader(Header header) {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE );
        //  Bytebuffer is BIG_ENDIAN by default
        //  Set to LITTLE_ENDIAN to make is similar to go impl
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        //System.out.println("header.timestamp="+ header.timestamp);
        buffer.putLong(header.timestamp);
        buffer.putInt(header.keySize);
        buffer.putInt(header.valSize);

        return buffer.array();
    }

    public static Header decodeHeader(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        long timestamp = buffer.getLong();
        int keySize = buffer.getInt();
        int valueSize = buffer.getInt();
        return new Header(timestamp, keySize, valueSize);
    }

    // Encodes the KV pair into bytes
    public static byte[] encodeKV(long timestamp, String key, String value) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        var header = encodeHeader(new Header(timestamp, keyBytes.length, valueBytes.length));

        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + keyBytes.length + valueBytes.length);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        buffer.put(header);
        buffer.put(keyBytes);
        buffer.put(valueBytes);

        return buffer.array();
    }

    public static KV decodeKV(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        Header header = decodeHeader(Arrays.copyOfRange(data, 0, HEADER_SIZE));

        long timestamp = header.timestamp;
        int keySize = header.keySize;
        int valSize = header.valSize;

        int offset = HEADER_SIZE;
        byte[] keyBytes = new byte[keySize];
        buffer.get(offset, keyBytes);
        offset += keySize;

        byte[] valueBytes = new byte[valSize];
        buffer.get(offset, valueBytes);

        String key = new String(keyBytes, StandardCharsets.UTF_8);
        String value = new String(valueBytes, StandardCharsets.UTF_8);

        return new KV(timestamp, key, value);
    }
}

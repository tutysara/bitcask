package net.tutysara.db;

// format file provides encode/decode functions for serialisation and deserialisation
// operations
//
// format methods are generic and does not have any disk or memory specific code.
//
// The disk storage deals with bytes; you cannot just store a string or object without
// converting it to bytes. The programming languages provide abstractions where you
// don't have to think about all this when storing things in memory (i.e. RAM).
// Consider the following example where you are storing stuff in a hash table:
//
//    books = {}
//    books["hamlet"] = "shakespeare"
//    books["anna karenina"] = "tolstoy"
//
// In the above, the language deals with all the complexities:
//
//    - allocating space on the RAM so that it can store data of `books`
//    - whenever you add data to `books`, convert that to bytes and keep it in the memory
//    - whenever the size of `books` increases, move that to somewhere in the RAM so that
//      we can add new items
//
// Unfortunately, when it comes to disks, we have to do all this by ourselves, write
// code which can allocate space, convert objects to/from bytes and many other operations.
//
// This file has two functions which help us with serialisation of data.
//
//    encodeKV - takes the key value pair and encodes them into bytes
//    decodeKV - takes a bunch of bytes and decodes them into key value pairs
//
//**workshop note**
//
//For the workshop, the functions will have the following signature:
//
//    func encodeKV(timestamp uint32, key string, value string) (int, []byte)
//    func decodeKV(data []byte) (uint32, string, string)

import net.tutysara.db.datatype.U32;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;


public class Format {

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
// bytes occupied by the key and value. The maximum signed integer (Java's Integer.MAX_VALUE)
// stored by 4 bytes is 2,147,483,647 (2 ** 31 - 1), roughly ~2.1GB. So, the size of
// each key or value cannot exceed this. Theoretically, a single row can be as large
// as ~4.2GB.
    public static int HEADER_SIZE = 12;
    public static Charset CHAR_SET = StandardCharsets.UTF_8;

// KeyEntry keeps the metadata about the KV, specially the position of
// the byte offset in the file. Whenever we insert/update a key, we create a new
// KeyEntry object and insert that into keyDir.

    static byte[] encodeHeader(Header header) {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        buffer.put(header.timeStamp.bytes());
        buffer.putInt(header.keySize);
        buffer.putInt(header.valueSize);
        return buffer.array();
    }

    static Header decodeHeader(byte[] bytes) {
        assert bytes.length == HEADER_SIZE : "header size should be equal to " + HEADER_SIZE;
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte[] dest = new byte[4];
        buffer.get(dest);
        U32 timeStamp = U32.fromBytes(dest);
        int keySize = buffer.getInt();
        int valueSize = buffer.getInt();
        return new Header(timeStamp, keySize, valueSize);
    }

    public static byte[] encodeKV(long timestamp, String key, String value) {

        int keySize = key.getBytes(CHAR_SET).length;
        int valueSize = value.getBytes(CHAR_SET).length;

        Header header = new Header(U32.fromLong(timestamp), keySize, valueSize);
        byte[] headerBytes = encodeHeader(header);

        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + keySize + valueSize);
        buffer.put(headerBytes);
        buffer.put(key.getBytes(CHAR_SET));
        buffer.put(value.getBytes(CHAR_SET));
        return buffer.array();

    }

    public static DecoderResponse decodeKV(byte[] data) {
        Header header = decodeHeader(Arrays.copyOfRange(data, 0, HEADER_SIZE));

        String key = new String(
                Arrays.copyOfRange(data,
                        HEADER_SIZE,
                        HEADER_SIZE + header.keySize));
        String val = new String(Arrays.copyOfRange(data,
                HEADER_SIZE + header.keySize,
                data.length));
        return new DecoderResponse(header.timeStamp.toLong(), key, val);
    }

    record KeyEntry(long timestamp, long position, int totalSize) {
    }

    record Header(U32 timeStamp, int keySize, int valueSize) {
    }

    record DecoderResponse(long timestamp, String key, String value) {
        public int size() {
            return Format.HEADER_SIZE +
                    key.getBytes(Format.CHAR_SET).length +
                    value.getBytes(Format.CHAR_SET).length;
        }
    }
}

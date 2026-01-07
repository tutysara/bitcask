package net.tutysara.db.datatype;

import java.nio.ByteBuffer;
import java.util.Arrays;

// use 4 byte signed int as container for u32
//
//+----------+----+----+----------+------------------+------------+------------+------------------+
//| ig (msb) | ig | ig | ig (lsb) | u32 (msb), by[0] | u32, by[1] | u32, by[2] | u32 (lsb), by[3] |
//+----------+----+----+----------+------------------+------------+------------+------------------+
// ig = ignored
//
public record U32(int val) {

    public static long maxval() {
        return (long) Math.pow(2, 32) -1;
    }
    public static U32 fromLong(Long lval) {
        assert lval <= maxval(): "long val cannot be more than : " + maxval();
        assert lval >= 0 : "long val should be positive";
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(lval);
        var converted = buffer.array();
        var u32 = Arrays.copyOfRange(converted, 4, Long.BYTES);
        ByteBuffer buffer2 = ByteBuffer.wrap(u32);
        return new U32(buffer2.getInt());
    }

    public static U32 fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.put(bytes);
        buffer.flip();
        return new U32(buffer.getInt());
    }

    public long toLong() {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        byte[] zeroMSB = {0, 0, 0, 0};
        buffer.put(zeroMSB);
        buffer.put(bytes());
        buffer.flip();
        return buffer.getLong();
    }

    public byte[] bytes() {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(val);
        return buffer.array();
    }

    public void printBytes() {
        for (byte b : bytes()) {
            System.out.println(Byte.toUnsignedInt(b));
        }
    }
}

package net.tutysara.db.datatype;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class U32Test {

    @Test
    public void test_MaxVal_FromLong() {
        var maxVal = U32.maxval();
        var u32 = U32.fromLong(maxVal);
        var bytes = u32.bytes();
        assertEquals(255, Byte.toUnsignedInt(bytes[0]));
        assertEquals(maxVal, u32.toLong());
    }

    @Test
    public void test_Val_FromLong() {
        var u32 = U32.fromLong(5L);
        assertEquals(5, u32.toLong());
    }

    @Test
    public void test_Bytes() {
        var u32 = U32.fromLong(5L);
        var bytes = u32.bytes();
        assertEquals(0, bytes[0]);
        assertEquals(0, bytes[1]);
        assertEquals(0, bytes[2]);
        assertEquals(5, bytes[3]);
    }

    @Test
    public void test_Neg_Val() {
        AssertionError thrown = Assertions.assertThrows(AssertionError.class, () -> {
            var u32 = U32.fromLong(-5L);
        }, "This should throw an AssertionError for negative values");
        Assertions.assertNotNull(thrown);
    }
}
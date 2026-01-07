package net.tutysara.db;

import net.tutysara.db.datatype.U32;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;

import static net.tutysara.db.Format.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FormatTest {

    Format formatter;
    @Test
    void test_EncodeHeader() {
        int[][] tests = {
                {10, 10, 10},
                {0, 0, 0},
                {10000, 10000, 10000}
        };

        for (int[] test : tests) {
            var header = new Header(U32.fromLong((long)test[0]), test[1], test[2]);
            byte[] data = encodeHeader(header);
            var result = decodeHeader(data);
            assertEquals(test[0], result.timeStamp().toLong(), "timestamp mismatch");
            assertEquals(test[1], result.keySize(), "keySize mismatch");
            assertEquals(test[2], result.valueSize(), "valueSize mismatch");
        }
    }

    @Test
    void test_EncodeKV() {
        Object[][] tests = {
               // {10, "hello", "world", HEADER_SIZE + 10},
               // {0, "", "", HEADER_SIZE},
                {100, "🔑", "", HEADER_SIZE + "🔑".getBytes(StandardCharsets.UTF_8).length}
        };

        for (Object[] test : tests) {
            int ts = (int) test[0];
            String key = (String) test[1];
            String value = (String) test[2];
            int expectedSize = (int) test[3];

            byte[] encodedBytes = encodeKV(ts, key, value);
            var decoderResponse = decodeKV(encodedBytes);

            assertEquals(ts, decoderResponse.timestamp(), "timestamp mismatch");
            assertEquals(key, decoderResponse.key(), "key mismatch");
            assertEquals(value, decoderResponse.value(), "value mismatch");
            assertEquals(expectedSize, decoderResponse.size(), "size mismatch");
        }
    }

}


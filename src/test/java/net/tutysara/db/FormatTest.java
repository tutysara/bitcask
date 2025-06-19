package net.tutysara.db;

import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;

import static net.tutysara.db.Format.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FormatTest {
    @Test
    void testEncodeHeader() {
        int[][] tests = {
                {10, 10, 10},
                {0, 0, 0},
                {10000, 10000, 10000}
        };

        for (int[] test : tests) {
            var header = new Header(test[0], test[1], test[2]);
            byte[] data = encodeHeader(header);
            var result = decodeHeader(data);
            assertEquals(test[0], result.timestamp(), "timestamp mismatch");
            assertEquals(test[1], result.keySize(), "keySize mismatch");
            assertEquals(test[2], result.valSize(), "valueSize mismatch");
        }
    }

    @Test
    void testEncodeKV() {
        Object[][] tests = {
                {10, "hello", "world", HEADER_SIZE + 10},
                {0, "", "", HEADER_SIZE},
                {100, "🔑", "", HEADER_SIZE + "🔑".getBytes(StandardCharsets.UTF_8).length}
        };

        for (Object[] test : tests) {
            int ts = (int) test[0];
            String key = (String) test[1];
            String value = (String) test[2];
            int expectedSize = (int) test[3];

            var encoded = encodeKV(ts, key, value);
            var decoded = decodeKV(encoded);

            assertEquals(ts, decoded.timestamp(), "timestamp mismatch");
            assertEquals(key, decoded.key(), "key mismatch");
            assertEquals(value, decoded.value(), "value mismatch");
            assertEquals(expectedSize, encoded.length, "size mismatch");
        }
    }
}


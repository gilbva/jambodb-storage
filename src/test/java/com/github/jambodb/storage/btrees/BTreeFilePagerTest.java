package com.github.jambodb.storage.btrees;

import com.github.jambodb.storage.btrees.mock.StringSerializer;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.UUID;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class BTreeFilePagerTest extends BTreeTestBase {
    public static final Logger LOG = Logger.getLogger(BTreeFilePagerTest.class.getName());

    private static Serializer<String> STRING_SERIALIZER;

    @BeforeAll
    public static void beforeAll() {
        STRING_SERIALIZER = new StringSerializer();
    }

    @Test
    public void testBTreeFile() throws IOException {
        var expected = new TreeMap<String, String>();
        var path = Files.createTempDirectory("btree-test.data");
        var pager = new FilePager<>(4, path, STRING_SERIALIZER, STRING_SERIALIZER);
        BTree<String, String> btree = new BTree<>(pager);
        pager.fsync();

        for (int i = 0; i < 100; i++) {
            var key = UUID.randomUUID().toString().substring(0, 8);

            expected.put(key, String.valueOf(i));
        }

        var strQueries = Arrays.asList(new String[][]{
            {"a", "z"},
            {"0", "9"}
        });

        testBTree(expected, btree, strQueries);
    }
}

package com.github.jambodb.storage.pagers;

import com.github.jambodb.storage.btrees.BTree;
import com.github.jambodb.storage.btrees.BTreePage;
import com.github.jambodb.storage.btrees.BTreeTestBase;
import com.github.jambodb.storage.btrees.Serializer;
import com.github.jambodb.storage.btrees.mock.StringSerializer;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.UUID;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class BTreeFilePagerTest extends BTreeTestBase {
    public static final Logger LOG = Logger.getLogger(BTreeFilePagerTest.class.getName());

    private static Serializer<String> STRING_SERIALIZER;

    @BeforeAll
    public static void beforeAll() {
        STRING_SERIALIZER = new StringSerializer();
    }

    @Test
    public void testSplit() throws IOException {
        var path = Files.createTempFile("btree-test", ".data");
        var pager = new FilePager<String, String>(2, path, STRING_SERIALIZER, STRING_SERIALIZER);
        BTree<String, String> btree = new BTree<>(pager);
        btree.put("a", "A");
        btree.put("b", "B");
        btree.put("c", "C");
        BTreePage<String, String> page = pager.page(1);
        assertNotNull(page);
        assertEquals(0, page.child(0));
        assertEquals(2, page.child(1));
        assertEquals("b", page.key(0));
    }

    @Test
    public void testBTreeFile() throws IOException {
        var expected = new TreeMap<String, String>();
        var path = Files.createTempFile("btree-test", ".data");
        var pager = new FilePager<>(4, path, STRING_SERIALIZER, STRING_SERIALIZER);
        BTree<String, String> btree = new BTree<>(pager);
        pager.fsync();

        for (int i = 0; i < 10; i++) {
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

package com.github.jambodb.storage.btrees;

import com.github.jambodb.storage.pager.FilePager;
import com.github.jambodb.storage.types.IntegerSerializer;
import com.github.jambodb.storage.types.SmallStringSerializer;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.logging.Logger;

public class BTreeFilePagerTest extends BTreeTestBase {
    public static final Logger LOG = Logger.getLogger(BTreeFilePagerTest.class.getName());

    @TestFactory
    public Collection<DynamicTest> testBTree() {
        List<DynamicTest> lst = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            final int size = i;
            lst.add(DynamicTest.dynamicTest("testing btree size=" + size, () -> doTest(size)));
        }

        for (int i = 10000; i < 100000; i += 10000) {
            final int size = i;
            lst.add(DynamicTest.dynamicTest("testing btree size=" + size, () -> doTest(size)));
        }

        return lst;
    }

    private void doTest(int size) throws IOException {
        var tmpFile = Files.createTempFile("test", "jambodb");
        var strToIntPager = FilePager.create(tmpFile, SmallStringSerializer.INSTANCE, IntegerSerializer.INSTANCE);
        var intToStrPager = FilePager.create(tmpFile, IntegerSerializer.INSTANCE, SmallStringSerializer.INSTANCE);

        var expectedStiTree = new TreeMap<String, Integer>();
        var expectedItsTree = new TreeMap<Integer, String>();

        var strToInt = new BTree<>(strToIntPager);
        var intToStr = new BTree<>(intToStrPager);

        for (int i = 0; i < size; i++) {
            var str = UUID.randomUUID().toString().substring(0, 8);

            expectedStiTree.put(str, i);
            expectedItsTree.put(i, str);
        }

        var strQueries = Arrays.asList(new String[][]{
                {"a", "z"},
                {"0", "9"}
        });

        var intQueries = Arrays.asList(new Integer[][]{
                {0, 5},
                {6, 9}
        });

        testBTree(expectedStiTree, strToInt, strQueries);
        testBTree(expectedItsTree, intToStr, intQueries);
    }

}

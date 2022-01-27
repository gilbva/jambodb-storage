package com.github.jambodb.storage.btrees;

import com.github.jambodb.storage.btrees.mock.MockPager;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

public class BTreeMemPagerTest extends BTreeTestBase {
    public static final Logger LOG = Logger.getLogger(BTreeMemPagerTest.class.getName());

    @TestFactory
    public Collection<DynamicTest> testBTree() {
        List<DynamicTest> lst = new ArrayList<>();
        for (int md = 2; md < 100; md += 3) {
            final int maxDegree = md;
            for (int i = 0; i < 100; i++) {
                final int size = i;
                lst.add(DynamicTest.dynamicTest("testing btree md=" + maxDegree + " size=" + size, () -> doTest(maxDegree, size)));
            }
        }

        for (int md = 100; md < 300; md += 100) {
            final int maxDegree = md;
            for (int i = 10000; i < 100000; i += 10000) {
                final int size = i;
                lst.add(DynamicTest.dynamicTest("testing btree md=" + maxDegree + " size=" + size, () -> doTest(maxDegree, size)));
            }
        }

        return lst;
    }

    private void doTest(int md, int size) throws IOException {
        var expectedStiTree = new TreeMap<String, Integer>();
        var expectedItsTree = new TreeMap<Integer, String>();

        var strToInt = new BTree<>(new MockPager<String, Integer>(md));
        var intToStr = new BTree<>(new MockPager<Integer, String>(md));

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

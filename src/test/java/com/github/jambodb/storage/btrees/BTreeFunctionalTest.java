package com.github.jambodb.storage.btrees;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class BTreeFunctionalTest {
    public static final Logger LOG = Logger.getLogger(BTreeFunctionalTest.class.getName());

    @TestFactory
    public Collection<DynamicTest> testBTree() throws IOException {
        List<DynamicTest> lst = new ArrayList<>();
        for(int md = 3; md < 100; md+=3) {
            final int maxDegree = md;
            for(int i = 4; i < 20; i++) {
                final int size = i;
                lst.add(DynamicTest.dynamicTest("testing btree md=" + maxDegree + " size=" + size, () -> testBTree(maxDegree, size)));
            }
            for(int i = 1000; i < 10000; i+=1000) {
                final int size = i;
                lst.add(DynamicTest.dynamicTest("testing btree md=" + maxDegree + " size=" + size, () -> testBTree(maxDegree, size)));
            }
        }
        return lst;
    }

    private void testBTree(int md, int size) throws IOException {
        var expectedStiTree = new TreeMap<String, Integer>();
        var expectedItsTree = new TreeMap<Integer, String>();

        var strToInt = new BTree<>(new MockPager<String, Integer>(md));
        var intToStr = new BTree<>(new MockPager<Integer, String>(md));
        for(int i = 0; i < size; i++) {
            var str = "k" + i;//UUID.randomUUID().toString().substring(0, 8);

            Assertions.assertNull(intToStr.get(i));
            Assertions.assertNull(strToInt.get(str));

            strToInt.put(str, i);
            intToStr.put(i, str);

            Assertions.assertEquals(str, intToStr.get(i));
            Assertions.assertEquals(i, strToInt.get(str));

            strToInt.remove(str);
            intToStr.remove(i);

            Assertions.assertNull(intToStr.get(i));
            Assertions.assertNull(strToInt.get(str));

            strToInt.put(str, i);
            intToStr.put(i, str);

            Assertions.assertEquals(str, intToStr.get(i));
            Assertions.assertEquals(i, strToInt.get(str));

            expectedStiTree.put(str, strToInt.get(str));
            expectedItsTree.put(i, intToStr.get(i));
        }

        assertEqualTrees(expectedItsTree, intToStr, null, null);
        assertEqualTrees(expectedItsTree.subMap(0, true, 5, true), intToStr, 0, 5);
        assertEqualTrees(expectedItsTree.subMap(6, true, 9, true), intToStr, 6, 9);

        assertEqualTrees(expectedStiTree, strToInt, null, null);
        assertEqualTrees(expectedStiTree.subMap("a", true, "z", true), strToInt, "a", "z");
        assertEqualTrees(expectedStiTree.subMap("0", true, "9", true), strToInt, "0", "9");
    }

    private <K extends Comparable<K>, V> void assertEqualTrees(NavigableMap<K, V> tree, BTree<K, V> btree, K from, K to) throws IOException {
        var expected = tree
                .keySet()
                .toArray();
        var current = toList(btree.query(from, to))
                .stream()
                .map(BTreeEntry::key)
                .collect(Collectors.toList())
                .toArray();

        if(expected.length != current.length) {
            LOG.log(Level.INFO,"{0} -> {1}", new Object[] { Arrays.toString(expected), Arrays.toString(current) });
            current = toList(btree.query(from, to))
                    .stream()
                    .map(BTreeEntry::key)
                    .collect(Collectors.toList())
                    .toArray();
        }
        Assertions.assertArrayEquals(expected, current, Arrays.toString(expected) + " => " + Arrays.toString(current));
    }

    private int count(Iterator<?> query) {
        int count = 0;
        while (query.hasNext()) {
            query.next();
            count++;
        }
        return count;
    }

    private <T extends BTreeEntry> List<T> toList(Iterator<T> query) {
        List<T> lst = new ArrayList<>();
        while (query.hasNext()) {
            var value = query.next();
            Assertions.assertNotNull(value);
            Assertions.assertNotNull(value.key());
            lst.add(value);
        }
        return lst;
    }
}

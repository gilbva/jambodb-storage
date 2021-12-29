package com.github.jambodb.storage.btrees;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class BTreeTest {
    @TestFactory
    public Collection<DynamicTest> testBTree() throws IOException {
        List<DynamicTest> lst = new ArrayList<>();
        for(int md = 3; md < 100; md+=3) {
            final int maxDegree = md;
            for(int i = 0; i < 20; i++) {
                final int size = i;
                lst.add(DynamicTest.dynamicTest("testing btree md=" + maxDegree + " size=" + size, () -> testBTreeOperations(maxDegree, size)));
            }
            for(int i = 1000; i < 10000; i+=1000) {
                final int size = i;
                lst.add(DynamicTest.dynamicTest("testing btree md=" + maxDegree + " size=" + size, () -> testBTreeOperations(maxDegree, size)));
            }
        }
        return lst;
    }

    private void testBTreeOperations(int md, int size) throws IOException {
        var stiTree = new TreeMap<String, Integer>();
        var itsTree = new TreeMap<Integer, String>();

        var strToInt = new BTree<>(new MockPager<String, Integer>(md));
        var intToStr = new BTree<>(new MockPager<Integer, String>(md));
        for(int i = 0; i < size; i++) {
            var str = UUID.randomUUID().toString().substring(0, 8);

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

            stiTree.put(str, strToInt.get(str));
            itsTree.put(i, intToStr.get(i));
        }

        assertEqualTrees(stiTree, strToInt.query(null, null));
        assertEqualTrees(stiTree.subMap("a", true, "z", true), strToInt.query("a", "z"));
        assertEqualTrees(stiTree.subMap("0", true, "9", true), strToInt.query("0", "9"));
    }

    private void assertEqualTrees(NavigableMap<String, Integer> tree, Iterator<BTreeEntry<String, Integer>> query) {
        var expected = tree
                .keySet()
                .toArray();
        var current = toList(query)
                .stream()
                .map(BTreeEntry::key)
                .collect(Collectors.toList())
                .toArray();
        Assertions.assertArrayEquals(expected, current, Arrays.toString(expected) + " => " + Arrays.toString(current));
    }

    @Test
    public void testBTreeRange() throws IOException {
        var btree = new BTree<>(new MockPager<String, Integer>(6));

        btree.put("7", 1);
        btree.put("b", 2);

        var query = btree.query("a", "z");
        Assertions.assertEquals(1, count(query));

        query = btree.query("a", "z");
        Assertions.assertEquals("b", query.next().key());
        Assertions.assertEquals(0, count(query));
    }

    @Test
    public void testBTreeRangeComplex() throws IOException {
        var btree = new BTree<>(new MockPager<String, Integer>(6));

        btree.put("0a", 1);
        btree.put("9a", 2);
        btree.put("aa", 3);
        btree.put("zz", 4);

        var tree = new TreeMap<String, Integer>();
        tree.put("0a", 1);
        tree.put("9a", 2);
        tree.put("aa", 3);
        tree.put("zz", 4);

        var subMap = tree.subMap("a", true, "z", true);
        var query = btree.query("a", "z");
        Assertions.assertEquals(subMap.size(), count(query));
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

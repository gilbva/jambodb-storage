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
        for (int md = 3; md < 100; md += 3) {
            final int maxDegree = md;
            for (int i = 4; i < 20; i++) {
                final int size = i;
                lst.add(DynamicTest.dynamicTest("testing btree md=" + maxDegree + " size=" + size, () -> testBTree(maxDegree, size)));
            }
            for (int i = 1000; i < 10000; i += 1000) {
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

    private <K extends Comparable<K>, V> void testBTree(TreeMap<K, V> tree, BTree<K, V> bTree, List<K[]> queries) throws IOException {
        for (var entry : tree.entrySet()) {
            bTree.put(entry.getKey(), entry.getValue());
            Assertions.assertEquals(entry.getValue(), bTree.get(entry.getKey()));
        }

        for (var entry : tree.entrySet()) {
            Assertions.assertEquals(entry.getValue(), bTree.get(entry.getKey()));
        }

        for (var key : tree.keySet()) {
            bTree.remove(key);
            Assertions.assertNull(bTree.get(key));
        }

        for (var key : tree.keySet()) {
            Assertions.assertNull(bTree.get(key));
        }

        for (var entry : tree.entrySet()) {
            bTree.put(entry.getKey(), entry.getValue());
            Assertions.assertEquals(entry.getValue(), bTree.get(entry.getKey()));
        }

        assertQuery(tree, bTree, null, null);
        for (var query : queries) {
            assertQuery(tree.subMap(query[0], true, query[1], true), bTree, query[0], query[1]);
        }
    }

    private <K extends Comparable<K>, V> void assertQuery(NavigableMap<K, V> tree, BTree<K, V> btree, K from, K to) throws IOException {
        var expected = tree
                .keySet()
                .toArray();
        var current = toList(btree.query(from, to))
                .stream()
                .map(BTreeEntry::key)
                .collect(Collectors.toList())
                .toArray();

        // debug
        if (expected.length != current.length) {
            LOG.log(Level.INFO, "{0} -> {1}", new Object[]{Arrays.toString(expected), Arrays.toString(current)});
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

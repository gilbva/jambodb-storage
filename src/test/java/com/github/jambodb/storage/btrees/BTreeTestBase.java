package com.github.jambodb.storage.btrees;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.Assertions;

public class BTreeTestBase {
    public static final Logger LOG = Logger.getLogger(BTreeTestBase.class.getName());

    protected <K extends Comparable<K>, V> void testBTree(TreeMap<K, V> tree, BTree<K, V> bTree, List<K[]> queries) throws IOException {
        var pager = bTree.getPager();
        for (var entry : tree.entrySet()) {
            bTree.put(entry.getKey(), entry.getValue());
            Assertions.assertNotNull(entry.getKey());
            V value = bTree.get(entry.getKey());
            Assertions.assertEquals(entry.getValue(), value);
        }
        pager.fsync();

        for (var entry : tree.entrySet()) {
            V value = bTree.get(entry.getKey());
            Assertions.assertEquals(entry.getValue(), value);
        }

        for (var key : tree.keySet()) {
            bTree.remove(key);
            if(bTree.get(key) != null) {
                System.out.println(key + " => " + bTree.get(key));
            }
            Assertions.assertNull(bTree.get(key));
        }

        System.out.println("before sync");
        for (var key : tree.keySet()) {
            if(bTree.get(key) != null) {
                System.out.println(key + " => " + bTree.get(key));
            }
            Assertions.assertNull(bTree.get(key));
        }
        pager.fsync();

        System.out.println("after sync");
        for (var key : tree.keySet()) {
            if(bTree.get(key) != null) {
                System.out.println(key + " => " + bTree.get(key));
            }
            Assertions.assertNull(bTree.get(key));
        }

        for (var entry : tree.entrySet()) {
            bTree.put(entry.getKey(), entry.getValue());
            V value = bTree.get(entry.getKey());
            Assertions.assertEquals(entry.getValue(), value);
        }
        pager.fsync();

        assertQuery(tree, bTree, null, null);
        for (var query : queries) {
            var expected = tree.subMap(query[0], true, query[1], true);
            assertQuery(expected, bTree, query[0], query[1]);
        }
    }

    private <K extends Comparable<K>, V> void assertQuery(NavigableMap<K, V> tree, BTree<K, V> btree, K from, K to) throws IOException {
        var expected = tree
                .keySet()
                .toArray();
        var current = toList(btree.query(from, to))
                .stream()
                .map(BTreeEntry::key).toArray();

        // debug
        if (expected.length != current.length) {
            LOG.log(Level.INFO, "{0} -> {1}", new Object[]{Arrays.toString(expected), Arrays.toString(current)});
            current = toList(btree.query(from, to))
                    .stream()
                    .map(BTreeEntry::key).toArray();
        }

        Assertions.assertArrayEquals(expected, current, Arrays.toString(expected) + " => " + Arrays.toString(current));
    }

    private <T extends BTreeEntry<?, ?>> List<T> toList(Iterator<T> query) {
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

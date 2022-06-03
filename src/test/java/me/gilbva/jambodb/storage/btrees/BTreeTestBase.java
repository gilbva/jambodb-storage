package me.gilbva.jambodb.storage.btrees;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;

public class BTreeTestBase {
    public static final Logger LOG = Logger.getLogger(BTreeTestBase.class.getName());

    protected <K extends Comparable<K>, V> void testBTree(TreeMap<K, V> tree, BTree<K, V> bTree, List<K[]> queries) throws IOException {
        Assertions.assertTrue(isEmpty(bTree));

        var pager = bTree.getPager();
        for (var entry : tree.entrySet()) {
            bTree.put(entry.getKey(), entry.getValue());
            Assertions.assertNotNull(entry.getKey());
            V value = bTree.get(entry.getKey());
            Assertions.assertEquals(entry.getValue(), value);
        }
        pager.fsync();

        for (var entry : tree.entrySet()) {
            Assertions.assertTrue(bTree.exists(entry.getKey()));
            V value = bTree.get(entry.getKey());
            Assertions.assertEquals(entry.getValue(), value);
        }

        for (var key : tree.keySet()) {
            Assertions.assertTrue(bTree.exists(key));
            bTree.remove(key);
            Assertions.assertFalse(bTree.exists(key));
            Assertions.assertNull(bTree.get(key));
        }

        Assertions.assertTrue(isEmpty(bTree));
        for (var key : tree.keySet()) {
            Assertions.assertFalse(bTree.exists(key));
            Assertions.assertNull(bTree.get(key));
        }

        pager.fsync();
        Assertions.assertTrue(isEmpty(bTree));
        for (var key : tree.keySet()) {
            Assertions.assertFalse(bTree.exists(key));
            Assertions.assertNull(bTree.get(key));
        }

        for (var entry : tree.entrySet()) {
            bTree.put(entry.getKey(), entry.getValue());
            Assertions.assertEquals(entry.getValue(), bTree.get(entry.getKey()));
        }

        assertQuery(tree, bTree, null, null);
        for (var query : queries) {
            var expected = tree.subMap(query[0], true, query[1], true);
            assertQuery(expected, bTree, query[0], query[1]);
        }

        pager.fsync();
        assertQuery(tree, bTree, null, null);
        for (var query : queries) {
            var expected = tree.subMap(query[0], true, query[1], true);
            assertQuery(expected, bTree, query[0], query[1]);
        }
    }

    protected <K extends Comparable<K>, V> void assertQuery(NavigableMap<K, V> tree, BTree<K, V> btree, K from, K to) throws IOException {
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

    protected <K extends Comparable<K>, V> boolean isEmpty(BTree<K, V> btree) throws IOException {
       return btree.query(null, null).hasNext();
    }

    protected <T extends BTreeEntry<?, ?>> List<T> toList(Iterator<T> query) {
        List<T> lst = new ArrayList<>();
        while (query.hasNext()) {
            var value = query.next();
            Assertions.assertNotNull(value);
            Assertions.assertNotNull(value.key());
            lst.add(value);
        }
        return lst;
    }

    protected <K extends Comparable<K>, V> void removeAll(BTree<K, V> btree) throws IOException {
        var current = toList(btree.query(null, null))
                .stream()
                .map(BTreeEntry::key).collect(Collectors.toList());
        for (var key : current) {
            Assertions.assertTrue(btree.exists(key));
            btree.remove(key);
            Assertions.assertFalse(btree.exists(key));
            Assertions.assertNull(btree.get(key));
        }
        Assertions.assertTrue(isEmpty(btree));
        btree.getPager().fsync();
        Assertions.assertTrue(isEmpty(btree));
    }
}

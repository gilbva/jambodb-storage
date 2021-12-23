package com.github.jambodb.storage.btrees;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.util.*;

public class BTreeTest {
    @TestFactory
    public Collection<DynamicTest> testBTree() throws IOException {
        List<DynamicTest> lst = new ArrayList<>();
        for(int md = 3; md < 100; md+=3) {
            final int maxDegree = md;
            for(int i = 0; i < 20; i++) {
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
        var strToIntTree = new TreeMap<String, Integer>();
        var intToStrTree = new TreeMap<String, Integer>();

        var strToInt = new BTree<>(new MockPager<String, Integer>(md));
        var intToStr = new BTree<>(new MockPager<Integer, String>(md));
        for(int i = 0; i < size; i++) {
            var str = UUID.randomUUID().toString();

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

            strToIntTree.put(str, strToInt.get(str));
            intToStr.put(i, intToStr.get(i));
        }

        var bTreeIt = strToInt.query("a", "z");
        int count = 0;
        while (bTreeIt.hasNext()) {
            bTreeIt.next();
            count++;
        }
        bTreeIt = strToInt.query("a", "z");
        var map = strToIntTree.subMap("a", true, "z", true);

        Assertions.assertEquals(map.size(), count);

        var treeIt = map.entrySet().iterator();
        while (bTreeIt.hasNext()) {
            Assertions.assertTrue(treeIt.hasNext());

            var expected = treeIt.next();
            var current = bTreeIt.next();

            System.out.println(expected.getKey() + " - " + current.key());
            Assertions.assertEquals(expected.getKey(), current.key());
            Assertions.assertEquals(expected.getValue(), current.value());
        }
    }
}

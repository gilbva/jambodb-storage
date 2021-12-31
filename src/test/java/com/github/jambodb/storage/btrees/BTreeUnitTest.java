package com.github.jambodb.storage.btrees;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class BTreeUnitTest {
    private Random random = new Random();

    @Test
    public void testMerge() throws IOException {
        for (int i = 1; i <= 100; i++) {
            testMerge(i, true);
            testMerge(i, false);
        }
    }

    public void testMerge(int size, boolean mergeRight) throws IOException {
        MockPager<String, Integer> pager = new MockPager<>(size * 2);
        BTree<String, Integer> btree = new BTree<>(pager);

        var parent = fillWithDummyData(pager.create(false), size);
        var rightPage = fillWithDummyData(pager.create(false), size);
        var leftPage = fillWithDummyData(pager.create(false), random.nextInt(size) + 1);

        int index = random.nextInt(parent.size());
        parent.child(index, leftPage.id());
        parent.child(index+1, rightPage.id());

        var targetExpectedKeys = new ArrayList<>(Arrays.asList(leftPage.getKeys()));
        var targetExpectedValues = new ArrayList<>(Arrays.asList(leftPage.getValues()));
        var targetExpectedChildren = new ArrayList<>(Arrays.asList(leftPage.getChildren()));

        targetExpectedKeys.add(parent.key(index));
        targetExpectedValues.add(parent.value(index));
        targetExpectedKeys.addAll(Arrays.asList(rightPage.getKeys()));
        targetExpectedValues.addAll(Arrays.asList(rightPage.getValues()));
        targetExpectedChildren.addAll(Arrays.asList(rightPage.getChildren()));

        var parentExpectedKeys = new ArrayList<>(Arrays.asList(parent.getKeys()));
        var parentExpectedValues = new ArrayList<>(Arrays.asList(parent.getValues()));
        var parentExpectedChildren = new ArrayList<>(Arrays.asList(parent.getChildren()));

        parentExpectedKeys.remove(index);
        parentExpectedValues.remove(index);
        parentExpectedChildren.remove(index+1);

        if(mergeRight) {
            btree.mergeRight(new BTree.Node<>(parent, index), leftPage);
        }
        else {
            btree.mergeLeft(new BTree.Node<>(parent, index+1), rightPage);
        }
        assertArrayEquals(parentExpectedKeys.toArray(), parent.getKeys());
        assertArrayEquals(parentExpectedValues.toArray(), parent.getValues());
        assertArrayEquals(parentExpectedChildren.toArray(), parent.getChildren());
        assertArrayEquals(targetExpectedKeys.toArray(), leftPage.getKeys());
        assertArrayEquals(targetExpectedValues.toArray(), leftPage.getValues());
        assertArrayEquals(targetExpectedChildren.toArray(), leftPage.getChildren());
    }

    @Test
    public void testMove() throws IOException {
        for (int i = 1; i <= 100; i++) {
            MockPager<String, Integer> pager = new MockPager<>(i);
            BTree<String, Integer> btree = new BTree<>(pager);

            var source = fillWithDummyData(pager.create(false), i);
            var target = fillWithDummyData(pager.create(false), 0);

            var sourceExpectedKeys = new ArrayList<>(Arrays.asList(source.getKeys()));
            var sourceExpectedValues = new ArrayList<>(Arrays.asList(source.getValues()));
            var sourceExpectedChildren = new ArrayList<>(Arrays.asList(source.getChildren()));

            var targetExpectedKeys = new ArrayList<>(Arrays.asList(target.getKeys()));
            var targetExpectedValues = new ArrayList<>(Arrays.asList(target.getValues()));
            var targetExpectedChildren = new ArrayList<>(Arrays.asList(target.getChildren()));

            while (source.size() > 0) {
                int index = random.nextInt(source.size());

                move(sourceExpectedKeys, targetExpectedKeys, index);
                move(sourceExpectedValues, targetExpectedValues, index);
                moveChildren(sourceExpectedChildren, targetExpectedChildren, index);

                btree.move(source, target, index);
                assertArrayEquals(sourceExpectedKeys.toArray(), source.getKeys());
                assertArrayEquals(sourceExpectedValues.toArray(), source.getValues());
                assertArrayEquals(sourceExpectedChildren.toArray(), source.getChildren());
                assertArrayEquals(targetExpectedKeys.toArray(), target.getKeys());
                assertArrayEquals(targetExpectedValues.toArray(), target.getValues());
                assertArrayEquals(targetExpectedChildren.toArray(), target.getChildren());
            }
        }
    }

    private void moveChildren(ArrayList<Object> source, ArrayList<Object> target, int index) {
        target.remove(target.size() - 1);
        target.addAll(source.subList(index, source.size()));
        while (index < source.size() - 1) {
            source.remove(source.size() - 1);
        }
    }

    private void move(ArrayList<Object> source, ArrayList<Object> target, int index) {
        target.addAll(source.subList(index, source.size()));
        while (index < source.size()) {
            source.remove(source.size() - 1);
        }
    }

    @Test
    public void testPromoteLast() throws IOException {
        MockPager<String, Integer> pager = new MockPager<>(3);
        BTree<String, Integer> btree = new BTree<>(pager);

        var parent = pager.create(false);
        var source = pager.create(false);

        parent.size(1);
        parent.key(0, "parent");
        parent.value(0, 1);
        parent.child(0, source.id());
        parent.child(1, 10);
        source.size(1);
        source.key(0, "source");
        source.value(0, 2);
        source.child(0, 20);
        source.child(1, 30);

        btree.promoteLast(source, new BTree.Node<>(parent, 0));
        assertEquals(2, parent.size());
        assertEquals("source", parent.key(0));
        assertEquals(2, parent.value(0));
        assertEquals("parent", parent.key(1));
        assertEquals(1, parent.value(1));
        assertEquals(source.id(), parent.child(1));
        assertEquals(10, parent.child(2));
        assertEquals(0, source.size());
    }

    @Test
    public void testRotations() throws IOException {
        MockPager<String, Integer> pager = new MockPager<>(3);
        BTree<String, Integer> btree = new BTree<>(pager);

        var parent = pager.create(false);
        var source = pager.create(false);
        var target = pager.create(false);

        parent.size(1);
        parent.key(0, "parent");
        parent.value(0, 1);
        parent.child(0, source.id());
        parent.child(1, target.id());
        source.size(1);
        source.key(0, "source");
        source.value(0, 2);
        source.child(0, 10);
        source.child(1, 20);
        target.size(0);
        target.child(0, 30);

        btree.rotateRight(new BTree.Node<>(parent, 0), source, target);
        assertEquals(1, parent.size());
        assertEquals("source", parent.key(0));
        assertEquals(2, parent.value(0));
        assertEquals(source.id(), parent.child(0));
        assertEquals(target.id(), parent.child(1));
        assertEquals(0, source.size());
        assertEquals(10, source.child(0));
        assertEquals(1, target.size());
        assertEquals("parent", target.key(0));
        assertEquals(1, target.value(0));
        assertEquals(20, target.child(0));
        assertEquals(30, target.child(1));

        btree.rotateLeft(new BTree.Node<>(parent, 0), target, source);
        assertEquals(1, parent.size());
        assertEquals("parent", parent.key(0));
        assertEquals(1, parent.value(0));
        assertEquals(source.id(), parent.child(0));
        assertEquals(target.id(), parent.child(1));
        assertEquals(1, source.size());
        assertEquals("source", source.key(0));
        assertEquals(2, source.value(0));
        assertEquals(10, source.child(0));
        assertEquals(20, source.child(1));
        assertEquals(0, target.size());
        assertEquals(30, target.child(0));
    }

    @Test
    public void testInsertPlace() throws IOException {
        for (int i = 1; i <= 26; i++) {
            MockPager<String, Integer> pager = new MockPager<>(i);
            BTree<String, Integer> btree = new BTree<>(pager);

            var expectedKeys = new ArrayList<>(i);
            var expectedValues = new ArrayList<>(i);
            var expectedChildren = new ArrayList<>(i);

            var nonLeafPage = pager.create(false);
            nonLeafPage.child(0, i * 10);
            expectedChildren.add(0, i * 10);

            while (expectedKeys.size() < i) {
                int index = random.nextInt(nonLeafPage.size()+1);

                String key = String.valueOf((char)('a' + index));
                int value = index + 1;
                int child = index * 10;

                expectedKeys.add(index, key);
                expectedValues.add(index, value);
                expectedChildren.add(index, child);

                btree.insertPlace(nonLeafPage, index);
                nonLeafPage.key(index, key);
                nonLeafPage.value(index, value);
                nonLeafPage.child(index, child);

                assertArrayEquals(expectedKeys.toArray(), nonLeafPage.getKeys());
                assertArrayEquals(expectedValues.toArray(), nonLeafPage.getValues());
                assertArrayEquals(expectedChildren.toArray(), nonLeafPage.getChildren());
            }
        }
    }

    @Test
    public void testDeletePlace() throws IOException {
        MockPager<String, Integer> pager = new MockPager<>(26);
        BTree<String, Integer> btree = new BTree<>(pager);

        for (int i = 1; i <= 26; i++) {
            var expectedKeys = new ArrayList<>(i);
            var expectedValues = new ArrayList<>(i);
            var expectedChildren = new ArrayList<>(i);

            var nonLeafPage = pager.create(false);
            nonLeafPage.size(i);
            for (int j = 0; j < i; j++) {
                nonLeafPage.key(j, String.valueOf((char)('a' + j)));
                nonLeafPage.value(j, j);
                nonLeafPage.child(j, j);

                expectedKeys.add(nonLeafPage.key(j));
                expectedValues.add(nonLeafPage.value(j));
                expectedChildren.add(nonLeafPage.child(j));
            }
            nonLeafPage.child(i, i);
            expectedChildren.add(i);

            while (nonLeafPage.size() > 0) {
                int index = random.nextInt(nonLeafPage.size());
                btree.deletePlace(nonLeafPage, index);
                expectedKeys.remove(index);
                expectedValues.remove(index);
                expectedChildren.remove(index);

                assertArrayEquals(expectedKeys.toArray(), nonLeafPage.getKeys());
                assertArrayEquals(expectedValues.toArray(), nonLeafPage.getValues());
                assertArrayEquals(expectedChildren.toArray(), nonLeafPage.getChildren());
            }
        }
    }

    @Test
    public void testGetChildPage() throws IOException {
        Pager<BTreePage<String, Object>> pager = new MockPager<>(3);
        BTree<String, Object> btree = new BTree<>(pager);

        var leafPage = pager.create(true);
        leafPage.size(1);
        var nonLeafPage = pager.create(false);
        nonLeafPage.size(1);

        nonLeafPage.child(0, 0);
        var childPage = btree.getChildPage(nonLeafPage, 0);
        assertEquals(0, childPage.id(), "getChildPage should retrieve page id (0) from the pager");

        nonLeafPage.child(0, 10);
        childPage = btree.getChildPage(nonLeafPage, 0);
        assertNull(childPage, "child page for an invalid page id (10) should be null");

        leafPage.child(0, 1);
        assertTrue(leafPage.isLeaf());
        assertThrows(IllegalArgumentException.class, () -> btree.getChildPage(leafPage, 0), "getChildPage should throw IllegalArgumentException if called on leaf page");
        assertThrows(IndexOutOfBoundsException.class, () -> btree.getChildPage(nonLeafPage, 10), "getChildPage should throw IndexOutOfBoundsException if called with and invalid index");
        assertThrows(IndexOutOfBoundsException.class, () -> btree.getChildPage(nonLeafPage, -1), "getChildPage should throw IndexOutOfBoundsException if called with and invalid index");
    }

    private MockBTreePage<String, Integer> fillWithDummyData(MockBTreePage<String, Integer> page, int size) {
        page.size(size);
        for(int i = 0; i < size; i++) {
            page.key(i, "k" + i);
            page.value(i, i * 100);
            page.child(i, i * 10);
        }
        page.child(size, size * 10);
        return page;
    }
}

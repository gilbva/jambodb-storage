package com.github.jambodb.storage.btrees;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MockPager<K, V> implements Pager<BTreePage<K, V>> {

    private int root;

    private Map<Integer, MockBTreePage<K, V>> map;

    private int lastPage;

    private int maxDegree;

    public MockPager(int maxDegree) {
        this.maxDegree = maxDegree;
        this.map = new HashMap<>();
        this.map.put(0, new MockBTreePage<>(0, maxDegree, true));
    }

    @Override
    public int root() throws IOException {
        return root;
    }

    @Override
    public void root(int id) throws IOException {
        root = id;
    }

    @Override
    public MockBTreePage<K, V> page(int id) throws IOException {
        return map.get(id);
    }

    @Override
    public MockBTreePage<K, V> create(boolean leaf) throws IOException {
        lastPage++;
        var page = new MockBTreePage<K, V>(lastPage, maxDegree, leaf);
        map.put(lastPage, page);
        return page;
    }

    @Override
    public void remove(int id) throws IOException {
        map.remove(id);
    }

    @Override
    public void fsync() throws IOException {

    }
}

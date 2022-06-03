package me.gilbva.jambodb.storage.btrees.mock;

import me.gilbva.jambodb.storage.btrees.BTreePage;
import me.gilbva.jambodb.storage.btrees.Pager;

import java.util.HashMap;
import java.util.Map;

public class MockPager<K, V> implements Pager<BTreePage<K, V>> {
    private final Map<Integer, MockBTreePage<K, V>> map;
    private final int maxDegree;
    private int root;
    private int lastPage;

    public MockPager(int maxDegree) {
        this.maxDegree = maxDegree;
        this.map = new HashMap<>();
        this.map.put(0, new MockBTreePage<>(0, maxDegree, true));
    }

    @Override
    public int root() {
        return root;
    }

    @Override
    public void root(int id) {
        root = id;
    }

    @Override
    public MockBTreePage<K, V> page(int id) {
        return map.get(id);
    }

    @Override
    public MockBTreePage<K, V> create(boolean leaf) {
        lastPage++;
        var page = new MockBTreePage<K, V>(lastPage, maxDegree, leaf);
        map.put(lastPage, page);
        return page;
    }

    @Override
    public void remove(int id) {
        map.remove(id);
    }

    @Override
    public void fsync() {

    }
}

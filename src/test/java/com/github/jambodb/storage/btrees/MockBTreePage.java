package com.github.jambodb.storage.btrees;

public class MockBTreePage<K, V> implements BTreePage<K, V> {
    private final int id;
    private final int maxDegree;
    private final K[] keys;
    private final V[] values;
    private final int[] children;
    private int size;

    private final boolean leaf;

    public MockBTreePage(int id, int maxDegree, boolean leaf) {
        this.id = id;
        this.maxDegree = maxDegree;
        this.leaf = leaf;
        keys = (K[]) new Object[maxDegree + 1];
        values = (V[]) new Object[maxDegree + 1];
        children = new int[maxDegree + 2];
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void size(int size) {
        this.size = size;
    }

    @Override
    public boolean isLeaf() {
        return leaf;
    }

    @Override
    public boolean isFull() {
        return this.size > maxDegree;
    }

    @Override
    public boolean isHalf() {
        return this.size < (maxDegree / 2) - 1;
    }

    @Override
    public boolean canBorrow() {
        return this.size >= (maxDegree / 2);
    }

    @Override
    public K key(int index) {
        return keys[index];
    }

    @Override
    public void key(int index, K key) {
        keys[index] = key;
    }

    @Override
    public V value(int index) {
        return values[index];
    }

    @Override
    public void value(int index, V value) {
        values[index] = value;
    }

    @Override
    public int child(int index) {
        return children[index];
    }

    @Override
    public void child(int index, int child) {
        children[index] = child;
    }
}

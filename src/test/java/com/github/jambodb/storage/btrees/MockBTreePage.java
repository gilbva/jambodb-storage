package com.github.jambodb.storage.btrees;

import java.util.Arrays;

public class MockBTreePage<K, V> implements BTreePage<K, V> {

    private int id;

    private Object[] keys;

    private Object[] values;

    private int[] children;

    private int maxDegree;

    private int size;

    private boolean leaf;

    public MockBTreePage(int id, int maxDegree, boolean leaf) {
        this.id = id;
        this.maxDegree = maxDegree;
        this.leaf = leaf;
        keys = new Object[maxDegree + 1];
        values = new Object[maxDegree + 1];
        children = new int[maxDegree + 2];
        Arrays.fill(children, -1);
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
        if (size < 0) {
            throw new IllegalArgumentException("invalid size " + size);
        }
        this.size = size;
        clean();
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
        return (K) keys[index];
    }

    @Override
    public void key(int index, K key) {
        keys[index] = key;
    }

    @Override
    public V value(int index) {
        return (V) values[index];
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

    @Override
    public String toString() {
        return "page:" + id;
    }

    public Object[] getKeys() {
        return getCleared(keys);
    }

    public Object[] getValues() {
        return getCleared(values);
    }

    public Object[] getChildren() {
        Object[] result = new Object[size + 1];
        for (int i = 0; i < result.length; i++) {
            result[i] = children[i];
        }
        return result;
    }

    private Object[] getCleared(Object[] array) {
        Object[] result = new Object[size];
        for (int i = 0; i < result.length; i++) {
            result[i] = array[i];
        }
        return result;
    }

    private void clean() {
        for (int i = size + 1; i < children.length; i++) {
            children[i] = -1;
        }
        for (int i = size; i < keys.length; i++) {
            keys[i] = null;
            values[i] = null;
        }
    }
}

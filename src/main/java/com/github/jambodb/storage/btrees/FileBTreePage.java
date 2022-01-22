package com.github.jambodb.storage.btrees;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.utils.SerializableWrapperList;
import java.io.IOException;
import java.util.Arrays;

public class FileBTreePage<K, V> implements BTreePage<K, V> {
    private final int id;
    private final boolean leaf;
    private final int maxDegree;
    private final int[] children;
    private final SerializableWrapperList<K> keys;
    private final SerializableWrapperList<V> values;

    private int size;
    private transient boolean dirty;

    public FileBTreePage(int id, boolean leaf, int maxDegree,
                         Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.id = id;
        this.leaf = leaf;
        this.maxDegree = maxDegree;
        keys = new SerializableWrapperList<>(keySerializer);
        values = new SerializableWrapperList<>(valueSerializer);
        children = new int[maxDegree + 2];
        Arrays.fill(children, -1);
        size = 0;
        dirty = true;
    }

    public static <K, V> FileBTreePage<K, V> read(int id, BlockStorage blockStorage,
                                                  Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        FileBTreePage<K, V> page = SerializerUtils.read(blockStorage, id + 1, FileBTreePage.class);
        page.keys.sync(keySerializer);
        page.values.sync(valueSerializer);
        page.dirty = false;
        return page;
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public int size() {
        return size;
    }

    public boolean dirty() {
        return dirty;
    }

    @Override
    public void size(int size) {
        this.size = size;
        for (int i = size; i <= maxDegree; i++) {
            children[i + 1] = -1;
        }
        dirty = true;
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
        return keys.get(index);
    }

    @Override
    public void key(int index, K key) {
        keys.put(index, key);
        expandSize(index);
        dirty = true;
    }

    @Override
    public V value(int index) {
        return values.get(index);
    }

    @Override
    public void value(int index, V value) {
        values.put(index, value);
        expandSize(index);
        dirty = true;
    }

    @Override
    public int child(int index) {
        if (index < children.length) {
            return children[index];
        }
        return -1;
    }

    @Override
    public void child(int index, int child) {
        if (index < children.length) {
            children[index] = child;
        }
        expandSize(index);
        dirty = true;
    }

    private void expandSize(int index) {
        if (index >= size) {
            size = index + 1;
        }
    }

    public void fsync(BlockStorage blockStorage) throws IOException {
        keys.prepare(blockStorage.blockSize());
        values.prepare(blockStorage.blockSize());
        int index;
        int storageId = id + 1;
        do {
            index = blockStorage.createBlock() - 1;
        } while (index < storageId);
        SerializerUtils.write(blockStorage, storageId, this);
    }
}

package com.github.jambodb.storage.pagers;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.btrees.BTreePage;
import com.github.jambodb.storage.btrees.Serializer;
import com.github.jambodb.storage.utils.SerializableList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

public class FileBTreePage<K, V> implements BTreePage<K, V> {
    private final int id;
    private final boolean leaf;
    private final int maxDegree;
    private final int[] children;
    private final SerializableList<K> keys;
    private final SerializableList<V> values;

    private int size;
    private boolean dirty;
    private boolean deleted;

    public FileBTreePage(int id, boolean leaf, int maxDegree,
                         Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.id = id;
        this.leaf = leaf;
        this.maxDegree = maxDegree;
        keys = new SerializableList<>(keySerializer);
        values = new SerializableList<>(valueSerializer);
        children = new int[maxDegree + 2];
        Arrays.fill(children, -1);
        size = 0;
        dirty = true;
        deleted = false;
    }

    public static <K, V> FileBTreePage<K, V> read(int id, BlockStorage blockStorage,
                                                  Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(blockStorage.blockSize());
        blockStorage.read(storageId(id), buffer);
        boolean deleted = buffer.getInt() == 1;
        if (deleted) {
            return null;
        }
        boolean leaf = buffer.getInt() == 1;
        int maxDegree = buffer.getInt();
        FileBTreePage<K, V> page = new FileBTreePage<>(id, leaf, maxDegree, keySerializer, valueSerializer);
        page.size = buffer.getInt();
        page.dirty = false;
        page.deleted = false;
        // children
        int childrenSize = buffer.getInt();
        for (int i = 0; i < childrenSize; i++) {
            page.children[i] = buffer.getInt();
        }
        // keys
        int keysSize = buffer.getInt();
        for (int i = 0; i < keysSize; i++) {
            int nextKeySize = buffer.getInt();
            if (nextKeySize > 0) {
                byte[] keysBytes = new byte[nextKeySize];
                buffer.get(keysBytes);
                page.keys.add(keysBytes);
            } else {
                page.keys.add(null);
            }
        }
        // values
        int valuesSize = buffer.getInt();
        for (int i = 0; i < valuesSize; i++) {
            int nextValueSize = buffer.getInt();
            if (nextValueSize > 0) {
                byte[] valuesBytes = new byte[nextValueSize];
                buffer.get(valuesBytes);
                page.values.add(valuesBytes);
            } else {
                page.values.add(null);
            }
        }
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

    public void deleted(boolean deleted) {
        if (this.deleted != deleted) {
            dirty = true;
        }
        this.deleted = deleted;
    }

    @Override
    public void size(int size) {
        if (this.size != size) {
            dirty = true;
            this.size = size;
            for (int i = size + 1; i < children.length; i++) {
                children[i] = -1;
            }
            keys.resize(size);
            values.resize(size);
        }
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
        if (index < keys.size()) {
            keys.put(index, key);
            dirty = true;
        }
    }

    @Override
    public V value(int index) {
        return values.get(index);
    }

    @Override
    public void value(int index, V value) {
        if (values.put(index, value)) {
            dirty = true;
        }
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
            dirty = true;
        }
    }

    public void fsync(BlockStorage blockStorage) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(blockStorage.blockSize());
        buffer.putInt(deleted ? 1 : 0);
        buffer.putInt(leaf ? 1 : 0);
        buffer.putInt(maxDegree);
        buffer.putInt(size);
        // children
        buffer.putInt(children.length);
        for (int child : children) {
            buffer.putInt(child);
        }
        // keys
        buffer.putInt(keys.size());
        Iterator<byte[]> keysBytesIterator = keys.bytesIterator();
        while (keysBytesIterator.hasNext()) {
            byte[] bytes = keysBytesIterator.next();
            if (bytes != null) {
                buffer.putInt(bytes.length);
                buffer.put(bytes);
            } else {
                buffer.putInt(0);
            }
        }
        // values
        buffer.putInt(values.size());
        Iterator<byte[]> valuesBytesIterator = values.bytesIterator();
        while (valuesBytesIterator.hasNext()) {
            byte[] bytes = valuesBytesIterator.next();
            if (bytes != null) {
                buffer.putInt(bytes.length);
                buffer.put(bytes);
            } else {
                buffer.putInt(0);
            }
        }
        buffer.flip();
        blockStorage.write(storageId(id), buffer);
    }

    private static int storageId(int id) {
        return id + 1;
    }
}

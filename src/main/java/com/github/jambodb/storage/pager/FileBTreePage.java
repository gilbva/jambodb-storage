package com.github.jambodb.storage.pager;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.btrees.BTreePage;
import com.github.jambodb.storage.btrees.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FileBTreePage<K, V> implements BTreePage<K, V> {
    private static final int IS_LEAF = 1;

    private static final int FLAGS_POS = 0;

    private static final int SIZE_POS = 1;

    private static final int POINTERS_POS = 2;

    private int id;

    private ByteBuffer buffer;

    private BlockStorage storage;

    private Serializer<K> sKey;

    private Serializer<V> sValue;

    public FileBTreePage(BlockStorage storage, int id, Serializer<K> sKey, Serializer<V> sValue, boolean isLeaf) throws IOException {
        this.storage = storage;
        this.id = id;
        this.buffer = ByteBuffer.allocate(BlockStorage.BLOCK_SIZE);
        this.buffer.put(FLAGS_POS, initFlags(isLeaf));
        this.sKey = sKey;
        this.sValue = sValue;
    }

    public FileBTreePage(BlockStorage storage, int id, Serializer<K> sKey, Serializer<V> sValue) throws IOException {
        this.storage = storage;
        this.id = id;
        this.buffer = ByteBuffer.allocate(BlockStorage.BLOCK_SIZE);
        storage.read(id, buffer);
        this.sKey = sKey;
        this.sValue = sValue;
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public int size() {
        return this.buffer.get(SIZE_POS);
    }

    @Override
    public void size(int value) {
        this.buffer.put(SIZE_POS, (byte) value);
    }

    @Override
    public boolean isLeaf() {
        return (this.buffer.get(FLAGS_POS) & IS_LEAF) == 0;
    }

    @Override
    public boolean isFull() {
        return false;
    }

    @Override
    public boolean isHalf() {
        return false;
    }

    @Override
    public boolean canBorrow() {
        return true;
    }

    @Override
    public K key(int index) {
        return read(index, 0, sKey);
    }

    @Override
    public void key(int index, K data) {

    }

    @Override
    public V value(int index) {
        return read(index, 0, sValue);
    }

    @Override
    public void value(int index, V data) {

    }

    @Override
    public int child(int index) {
        return readPointer(index, 3);
    }

    @Override
    public void child(int index, int id) {

    }

    private byte initFlags(boolean isLeaf) {
        byte flags = (byte) 0;
        if (isLeaf) {
            flags |= (byte) IS_LEAF;
        }
        return flags;
    }

    public <T> T read(int absIndex, int relIndex, Serializer<T> ser) {
        byte dataPos = readPointer(absIndex, relIndex);
        buffer.position(dataPos);
        return ser.read(buffer);
    }

    public byte readPointer(int absIndex, int relIndex) {
        int elSize = 3;
        if (isLeaf()) {
            elSize = 2;
        }
        if(relIndex >= elSize - 1) {
            return -1;
        }
        int elPos = POINTERS_POS + (absIndex * elSize) + relIndex;
        return buffer.get(elPos);
    }
}
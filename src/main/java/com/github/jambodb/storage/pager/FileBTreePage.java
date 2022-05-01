package com.github.jambodb.storage.pager;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.btrees.BTreePage;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FileBTreePage<K, V> implements BTreePage<K, V> {
    private static final int IS_LEAF = 1;

    private static final int FLAGS_POS = 0;

    private static final int SIZE_POS = 1;

    private int id;

    private ByteBuffer buffer;

    public FileBTreePage(BlockStorage storage, int id) throws IOException {
        this.id = id;
        this.buffer = ByteBuffer.allocate(BlockStorage.BLOCK_SIZE);
        storage.read(id, buffer);
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
        this.buffer.put(SIZE_POS, (byte)value);
    }

    @Override
    public boolean isLeaf() {
        return (this.buffer.get(SIZE_POS) & IS_LEAF) == 0;
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
        return false;
    }

    @Override
    public K key(int index) {
        return null;
    }

    @Override
    public void key(int index, K data) {

    }

    @Override
    public V value(int index) {
        return null;
    }

    @Override
    public void value(int index, V data) {

    }

    @Override
    public int child(int index) {
        return 0;
    }

    @Override
    public void child(int index, int id) {

    }
}

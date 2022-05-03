package com.github.jambodb.storage.pager;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.btrees.BTreePage;
import com.github.jambodb.storage.btrees.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FileBTreePage<K, V> implements BTreePage<K, V> {
    private int id;

    private ByteBuffer buffer;

    private BlockStorage storage;

    private Serializer<K> sKey;

    private Serializer<V> sValue;

    @Override
    public int id() {
        return 0;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public void size(int value) {

    }

    @Override
    public boolean isLeaf() {
        return false;
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
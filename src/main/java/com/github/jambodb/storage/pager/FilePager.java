package com.github.jambodb.storage.pager;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.btrees.Pager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public class FilePager<K, V> implements Pager<FileBTreePage<K, V>> {
    private BlockStorage storage;

    private int root;

    public FilePager(Path file, boolean init) throws IOException {
        storage = BlockStorage.open(file, init);
        if(init) {
            root = storage.increase();
            writeRoot();
        }
        else {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            storage.readHead(buffer);
            root = buffer.getInt();
        }
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
    public FileBTreePage<K, V> page(int id) throws IOException {
        return null;
    }

    @Override
    public FileBTreePage<K, V> create(boolean leaf) throws IOException {
        return null;
    }

    @Override
    public void remove(int id) throws IOException {

    }

    @Override
    public void fsync() throws IOException {
        writeRoot();

    }

    public void writeRoot() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(root);
        storage.writeHead(buffer);
    }
}

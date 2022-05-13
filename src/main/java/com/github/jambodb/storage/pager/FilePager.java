package com.github.jambodb.storage.pager;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.btrees.BTreePage;
import com.github.jambodb.storage.btrees.Pager;
import com.github.jambodb.storage.btrees.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public class FilePager<K, V> implements Pager<BTreePage<K, V>> {
    private BlockStorage storage;

    private int root;

    public static <K, V> FilePager<K, V> create(Path file, Serializer<K> keySer, Serializer<V> valueSer) throws IOException {
        return new FilePager<>(file, true, keySer, valueSer);
    }

    public static <K, V> FilePager<K, V> load(Path file, Serializer<K> keySer, Serializer<V> valueSer) throws IOException {
        return new FilePager<>(file, false, keySer, valueSer);
    }

    private Serializer<K> keySer;

    private Serializer<V> valueSer;

    private FilePager(Path file, boolean init, Serializer<K> keySer, Serializer<V> valueSer) throws IOException {
        this.keySer = keySer;
        this.valueSer = valueSer;

        if(init) {
            storage = BlockStorage.create(file);
            root = storage.increase();
            writeRoot();
        }
        else {
            storage = BlockStorage.open(file);
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

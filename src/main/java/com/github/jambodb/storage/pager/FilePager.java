package com.github.jambodb.storage.pager;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.btrees.BTreePage;
import com.github.jambodb.storage.btrees.Pager;
import com.github.jambodb.storage.btrees.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class FilePager<K, V> implements Pager<BTreePage<K, V>> {
    public static <K, V> FilePager<K, V> create(Path file, Serializer<K> keySer, Serializer<V> valueSer) throws IOException {
        return new FilePager<>(file, true, keySer, valueSer);
    }

    public static <K, V> FilePager<K, V> load(Path file, Serializer<K> keySer, Serializer<V> valueSer) throws IOException {
        return new FilePager<>(file, false, keySer, valueSer);
    }

    private Map<Integer, FileBTreePage<K, V>> pagesCache;

    private BlockStorage storage;

    private int root;

    private Serializer<K> keySer;

    private Serializer<V> valueSer;

    private FilePager(Path file, boolean init, Serializer<K> keySer, Serializer<V> valueSer) throws IOException {
        this.keySer = keySer;
        this.valueSer = valueSer;
        this.pagesCache = new HashMap<>();

        if(init) {
            storage = BlockStorage.create(file);
            root = create(true).id();
            writeRoot();
        }
        else {
            storage = BlockStorage.open(file);
            root = readRoot();
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
        if(pagesCache.containsKey(id)) {
            return pagesCache.get(id);
        }

        var page = FileBTreePage.load(storage, id, keySer, valueSer);
        pagesCache.put(id, page);
        return page;
    }

    @Override
    public FileBTreePage<K, V> create(boolean leaf) throws IOException {
        var page = FileBTreePage.create(storage, leaf, keySer, valueSer);
        pagesCache.put(page.id(), page);
        return page;
    }

    @Override
    public void remove(int id) throws IOException {
        page(id).setDeleted(true);
    }

    @Override
    public void fsync() throws IOException {
        writeRoot();
        for (var page : pagesCache.values()) {
            if(page.isModified()) {
                page.save();
            }
        }
        pagesCache.clear();
    }

    public void writeRoot() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(root);
        storage.writeHead(buffer);
    }

    private int readRoot() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        storage.readHead(buffer);
        return buffer.getInt();
    }
}

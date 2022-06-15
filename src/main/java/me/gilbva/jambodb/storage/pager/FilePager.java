package me.gilbva.jambodb.storage.pager;

import me.gilbva.jambodb.storage.blocks.BlockStorage;
import me.gilbva.jambodb.storage.btrees.BTreePage;
import me.gilbva.jambodb.storage.btrees.Pager;
import me.gilbva.jambodb.storage.btrees.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class FilePager<K, V> implements Pager<BTreePage<K, V>> {
    public static <K, V> FilePager<K, V> create(Path file, int cachePages, Serializer<K> keySer, Serializer<V> valueSer) throws IOException {
        return new FilePager<>(file, cachePages, true, keySer, valueSer);
    }

    public static <K, V> FilePager<K, V> open(Path file, int cachePages, Serializer<K> keySer, Serializer<V> valueSer) throws IOException {
        return new FilePager<>(file, cachePages, false, keySer, valueSer);
    }

    public static <K, V> FilePager<K, V> create(Path file, int cachePages, Serializer<K> keySer, Serializer<V> valueSer, String password) throws IOException {
        return new FilePager<>(file, cachePages, true, keySer, valueSer, password);
    }

    public static <K, V> FilePager<K, V> open(Path file, int cachePages, Serializer<K> keySer, Serializer<V> valueSer, String password) throws IOException {
        return new FilePager<>(file, cachePages, false, keySer, valueSer, password);
    }

    private final LRUPagesCache<K, V> cache;

    private final Map<Integer, SlottedBTreePage<K, V>> txPages;

    private final BlockStorage storage;

    private int root;

    private final Serializer<K> keySer;

    private final Serializer<V> valueSer;

    private FilePager(Path file, int cachePages, boolean init, Serializer<K> keySer, Serializer<V> valueSer) throws IOException {
        this.keySer = keySer;
        this.valueSer = valueSer;
        this.cache = new LRUPagesCache<>(cachePages);
        this.txPages = new HashMap<>();

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

    private FilePager(Path file, int cachePages, boolean init, Serializer<K> keySer, Serializer<V> valueSer, String password) throws IOException {
        this.keySer = keySer;
        this.valueSer = valueSer;
        this.cache = new LRUPagesCache<>(cachePages);
        this.txPages = new HashMap<>();

        if(init) {
            storage = BlockStorage.create(file, password);
            root = create(true).id();
            writeRoot();
        }
        else {
            storage = BlockStorage.open(file, password);
            root = readRoot();
        }
    }

    @Override
    public int root() {
        return root;
    }

    @Override
    public void root(int id) {
        root = id;
    }

    @Override
    public SlottedBTreePage<K, V> page(int id) throws IOException {
        if(id == 0) {
            throw new IllegalArgumentException("invalid id " + id);
        }
        if(txPages.containsKey(id)) {
            return txPages.get(id);
        }
        if(cache.contains(id)) {
            return cache.get(id);
        }

        var page = SlottedBTreePage.open(this, id);
        cache.put(page);
        return page;
    }

    @Override
    public SlottedBTreePage<K, V> create(boolean leaf) throws IOException {
        var page = SlottedBTreePage.create(this, leaf);
        txPages.put(page.id(), page);
        return page;
    }

    @Override
    public void remove(int id) throws IOException {
        if(id == 0) {
            throw new IllegalArgumentException("invalid id " + id);
        }
        page(id).setDeleted(true);
    }

    @Override
    public void fsync() throws IOException {
        writeRoot();
        for (var page : txPages.values()) {
            if(page.isModified()) {
                page.save();
                cache.put(page);
            }
        }
        txPages.clear();
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

    public Serializer<K> getKeySer() {
        return keySer;
    }

    public Serializer<V> getValueSer() {
        return valueSer;
    }

    public BlockStorage getStorage() {
        return storage;
    }

    void pageModified(SlottedBTreePage<K, V> page) {
        if(page.isModified()) {
            cache.remove(page);
            txPages.put(page.id(), page);
        }
    }
}

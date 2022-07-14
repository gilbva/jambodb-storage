package me.gilbva.jambodb.storage.pager;

import me.gilbva.jambodb.storage.blocks.BlockStorage;
import me.gilbva.jambodb.storage.btrees.BTreePage;
import me.gilbva.jambodb.storage.btrees.Pager;
import me.gilbva.jambodb.storage.btrees.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class FilePager<K, V> implements Pager<BTreePage<K, V>> {
    public static <K, V> FilePagerBuilder<K, V> create(Serializer<K> keySer, Serializer<V> valueSer) {
        return new FilePagerOptions<>(true, keySer, valueSer);
    }

    public static <K, V> FilePagerBuilder<K, V> open(Serializer<K> keySer, Serializer<V> valueSer) {
        return new FilePagerOptions<>(false, keySer, valueSer);
    }

    private final LRUPagesCache<K, V> cache;

    private final Map<Integer, SlottedBTreePage<K, V>> txPages;

    private final BlockStorage storage;

    private ByteBuffer roots;

    private final Serializer<K> keySer;

    private final Serializer<V> valueSer;

    FilePager(FilePagerOptions<K, V> opts) throws IOException {
        this.keySer = opts.keySerializer();
        this.valueSer = opts.valueSerializer();
        this.cache = new LRUPagesCache<>(opts.cachePages());
        this.txPages = new HashMap<>();
        this.roots = ByteBuffer.allocate(BlockStorage.HEAD_SIZE);

        if(opts.init()) {
            storage = BlockStorage.create(opts.file(), opts.security());
        }
        else {
            storage = BlockStorage.open(opts.file(), opts.security());
            readRoots();
        }
    }

    @Override
    public int root(int index) {
        return roots.getInt(index * 4);
    }

    @Override
    public void root(int index, int id) {
        roots.putInt(index * 4, id);
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
        writeRoots();
        for (var page : txPages.values()) {
            if(page.isModified()) {
                page.save();
                cache.put(page);
            }
        }
        txPages.clear();
    }

    public void writeRoots() throws IOException {
        storage.writeHead(roots);
    }

    private void readRoots() throws IOException {
        storage.readHead(roots);
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

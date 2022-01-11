package com.github.jambodb.storage.btrees;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class FilePager<K, V> implements Pager<FileBTreePage<K, V>> {
    private final int maxDegree;
    private final Map<Integer, FileBTreePage<K, V>> map;
    private int root;
    private int lastPage;

    public FilePager(int maxDegree, File dir) {
        this.maxDegree = maxDegree;
        this.map = new HashMap<>();
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
    public FileBTreePage<K, V> page(int id) {
        return map.get(id);
    }

    @Override
    public FileBTreePage<K, V> create(boolean leaf) {
        FileBTreePage<K, V> page = new FileBTreePage<>(lastPage++, leaf, maxDegree);
        map.put(lastPage, page);
        return page;
    }

    @Override
    public void remove(int id) {
        map.remove(id);
        if (lastPage - id == 1) {
            lastPage--;
        }
    }

    @Override
    public void fsync(File dir, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        Collection<FileBTreePage<K, V>> pages = map.values();
        for (FileBTreePage<K, V> page : pages) {
            page.fsync(dir, keySerializer, valueSerializer);
        }
    }
}

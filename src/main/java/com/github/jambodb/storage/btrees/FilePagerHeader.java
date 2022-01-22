package com.github.jambodb.storage.btrees;

import com.github.jambodb.storage.blocks.BlockStorage;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class FilePagerHeader {
    private int root;
    private final int maxDegree;
    private int lastPage;
    private List<Integer> deletedPages;

    public FilePagerHeader(int root, int maxDegree, int lastPage) {
        this.root = root;
        this.maxDegree = maxDegree;
        this.lastPage = lastPage;
    }

    public static FilePagerHeader read(BlockStorage blockStorage) throws IOException {
        return SerializerUtils.read(blockStorage, 0, FilePagerHeader.class);
    }

    public int root() {
        return root;
    }

    public void root(int root) {
        this.root = root;
    }

    public int maxDegree() {
        return maxDegree;
    }

    public int lastPage() {
        return lastPage;
    }

    public List<Integer> deletedPages() {
        if (deletedPages == null) {
            deletedPages = new LinkedList<>();
        }
        return deletedPages;
    }

    public void incLastPage() {
        incLastPage(1);
    }

    public void incLastPage(int diff) {
        lastPage += diff;
    }

    public void removeDeletedPage(int id) {
        if (deletedPages().contains(id)) {
            deletedPages.remove(id);
        }
    }

    public void addDeletedPage(int id) {
        if (!deletedPages().contains(id)) {
            deletedPages.add(id);
        }
    }

    public void write(BlockStorage blockStorage) throws IOException {
        blockStorage.createBlock();
        SerializerUtils.write(blockStorage, 0, this);
    }
}

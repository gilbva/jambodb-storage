package com.github.jambodb.storage.pagers;

import com.github.jambodb.storage.blocks.BlockStorage;
import java.io.IOException;
import java.nio.ByteBuffer;
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
        ByteBuffer buffer = ByteBuffer.allocate(blockStorage.blockSize());
        blockStorage.read(0, buffer);
        int root = buffer.getInt();
        int maxDegree = buffer.getInt();
        int lastPage = buffer.getInt();
        FilePagerHeader header = new FilePagerHeader(root, maxDegree, lastPage);
        int deletedSize = buffer.getInt();
        for (int i = 0; i < deletedSize; i++) {
            int deletedId = buffer.getInt();
            header.deletedPages().add(deletedId);
        }
        return header;
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
            deletedPages.remove((Integer) id);
        }
    }

    public void addDeletedPage(int id) {
        if (!deletedPages().contains(id)) {
            deletedPages.add(id);
        }
    }

    public void write(BlockStorage blockStorage) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(blockStorage.blockSize());
        buffer.putInt(root);
        buffer.putInt(maxDegree);
        buffer.putInt(lastPage);
        buffer.putInt(deletedPages().size());
        for (int deletedId : deletedPages) {
            buffer.putInt(deletedId);
        }
        buffer.flip();
        blockStorage.write(0, buffer);
    }
}

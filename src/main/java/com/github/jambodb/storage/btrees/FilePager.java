package com.github.jambodb.storage.btrees;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.blocks.FileBlockStorage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class FilePager<K, V> implements Pager<FileBTreePage<K, V>> {
    private static final int INDEX_BLOCK_SIZE = 4;
    private static final int BLOCK_SIZE = 8 * 1024;
    private static final int MAX_CACHE = 10;

    private final Path path;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final int maxDegree;
    private final Map<Integer, FileBTreePage<K, V>> map;
    private final Queue<Integer> cachePages;
    private final List<Integer> deletedPages;
    private int root;
    private int lastPage;

    public FilePager(int maxDegree, Path path, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.maxDegree = maxDegree;
        this.path = path;
        this.map = new HashMap<>();
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        cachePages = new LinkedList<>();
        deletedPages = new LinkedList<>();
    }

    public FilePager(Path path, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        this.path = path;
        int[] blocks = readHeader();
        if (blocks.length < 3) {
            throw new IOException("Could not read pager header");
        }
        root = blocks[0];
        maxDegree = blocks[1];
        lastPage = blocks[2];
        cachePages = new LinkedList<>();
        deletedPages = new LinkedList<>();
        for (int i = 3; i < blocks.length; i++) {
            deletedPages.add(blocks[i]);
        }
        this.map = new HashMap<>();
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public int lastPage() {
        return lastPage;
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
    public FileBTreePage<K, V> page(int id) throws IOException {
        if (map.get(id) == null && !deletedPages.contains(id)) {
            FileBTreePage<K, V> page = new FileBTreePage<>(id, path, keySerializer, valueSerializer);
            map.put(id, page);
            addCache(id);
        }
        return map.get(id);
    }

    private void addCache(int id) {
        if (!cachePages.contains(id)) {
            cachePages.add(id);
        }
        while (cachePages.size() > MAX_CACHE) {
            Integer cacheId = cachePages.poll();
            FileBTreePage<K, V> page = map.get(cacheId);
            if (page == null || !page.dirty()) {
                map.remove(cacheId);
            }
        }
    }

    @Override
    public FileBTreePage<K, V> create(boolean leaf) {
        FileBTreePage<K, V> page
            = new FileBTreePage<>(lastPage, leaf, maxDegree, keySerializer, valueSerializer, BLOCK_SIZE);
        map.put(lastPage, page);
        if (deletedPages.contains(lastPage)) {
            deletedPages.remove(lastPage);
        }
        lastPage++;
        return page;
    }

    @Override
    public void remove(int id) {
        map.remove(id);
        cachePages.remove(id);
        if (!deletedPages.contains(id)) {
            deletedPages.add(id);
        }
        if (lastPage - id == 1) {
            lastPage--;
        }
    }

    @Override
    public void fsync() throws IOException {
        writeHeader();
        writePages();
        clearCache();
    }

    private void clearCache() {
        map.clear();
        cachePages.clear();
    }

    private int[] readHeader() throws IOException {
        Path file = getHeaderPath();
        if (!Files.exists(file)) {
            return new int[0];
        }
        BlockStorage blockStorage = new FileBlockStorage(file, StandardOpenOption.READ);
        int[] blocks = new int[blockStorage.blockCount()];
        for (int i = 0; i < blocks.length; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(blockStorage.blockSize());
            blockStorage.read(i, buffer);
            blocks[i] = buffer.getInt();
        }
        return blocks;
    }

    private void writeHeader() throws IOException {
        Path file = getHeaderPath();
        if (Files.exists(file)) {
            Files.delete(file);
        }
        Files.createFile(file);
        int[] blocks = new int[3 + deletedPages.size()];
        blocks[0] = root;
        blocks[1] = maxDegree;
        blocks[2] = lastPage;
        int blockIndex = 3;
        for (int page : deletedPages) {
            blocks[blockIndex++] = page;
        }
        BlockStorage blockStorage = new FileBlockStorage(INDEX_BLOCK_SIZE, file, StandardOpenOption.WRITE);
        for (int block : blocks) {
            int index = blockStorage.createBlock() - 1;
            ByteBuffer buffer = ByteBuffer.allocate(INDEX_BLOCK_SIZE);
            buffer.putInt(block);
            buffer.flip();
            blockStorage.write(index, buffer);
        }
    }

    private Path getHeaderPath() {
        return Paths.get(path.toUri()).resolve("index.jmb.dat");
    }

    private void writePages() throws IOException {
        Collection<FileBTreePage<K, V>> pages = map.values();
        for (FileBTreePage<K, V> page : pages) {
            if (page.dirty() || !cachePages.contains(page.id())) {
                page.fsync(path);
            }
        }
    }
}

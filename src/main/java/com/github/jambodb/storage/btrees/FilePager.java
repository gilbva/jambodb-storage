package com.github.jambodb.storage.btrees;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.blocks.FileBlockStorage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;

public class FilePager<K, V> implements Pager<FileBTreePage<K, V>> {
    private static final int INDEX_BLOCK_SIZE = 4;

    private final Path path;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final int blockSize;
    private final int maxDegree;
    private final Map<Integer, FileBTreePage<K, V>> map;
    private int root;
    private int lastPage;

    public FilePager(int maxDegree, Path path, Serializer<K> keySerializer, Serializer<V> valueSerializer, int blockSize) {
        this.maxDegree = maxDegree;
        this.path = path;
        this.map = new HashMap<>();
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.blockSize = blockSize;
    }

    public FilePager(Path path, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        this.path = path;
        int[] blocks = readHeader();
        if (blocks.length < 4) {
            throw new IOException("Could not read pager header");
        }
        root = blocks[0];
        maxDegree = blocks[1];
        lastPage = blocks[2];
        blockSize = blocks[3];
        this.map = new HashMap<>();
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        readPages();
    }

    public int lastPage() {
        return lastPage;
    }

    public int blockSize() {
        return blockSize;
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
        FileBTreePage<K, V> page
            = new FileBTreePage<>(lastPage++, leaf, maxDegree, keySerializer, valueSerializer, blockSize);
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
    public void fsync() throws IOException {
        writeHeader();
        writePages();
        cleanPages();
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
        int[] blocks = new int[4];
        blocks[0] = root;
        blocks[1] = maxDegree;
        blocks[2] = lastPage;
        blocks[3] = blockSize;
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

    private void readPages() {
        for (int i = 0; i < lastPage; i++) {
            try {
                FileBTreePage<K, V> page = new FileBTreePage<>(i, path, keySerializer, valueSerializer);
                map.put(i, page);
            } catch (IOException ignored) {
            }
        }
    }

    private void writePages() throws IOException {
        Collection<FileBTreePage<K, V>> pages = map.values();
        for (FileBTreePage<K, V> page : pages) {
            page.fsync(path);
        }
    }

    private void cleanPages() throws IOException {
        List<Integer> ids = new ArrayList<>();
        map.forEach((key, value) -> {
            if (value == null) {
                ids.add(key);
            }
        });
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, file -> {
            for (int id : ids) {
                if (file.getFileName().toString().endsWith(String.format("-%d.jmb.dat", id))) {
                    return true;
                }
            }
            return false;
        })) {
            for (Path entry : stream) {
                Files.delete(entry);
            }
        }
    }
}

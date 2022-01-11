package com.github.jambodb.storage.btrees;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.blocks.FileBlockStorage;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;

public class FilePager<K, V> implements Pager<FileBTreePage<K, V>> {
    private static final int INDEX_BLOCK_SIZE = 4;

    private final File dir;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final int maxDegree;
    private final Map<Integer, FileBTreePage<K, V>> map;
    private int root;
    private int lastPage;

    public FilePager(int maxDegree, File dir, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.maxDegree = maxDegree;
        this.dir = dir;
        this.map = new HashMap<>();
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public FilePager(File dir, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.dir = dir;
        int[] blocks = readHeader(dir);
        root = blocks[0];
        maxDegree = blocks[1];
        lastPage = blocks[2];
        this.map = new HashMap<>();
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        readPages(dir, keySerializer, valueSerializer);
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
        FileBTreePage<K, V> page = new FileBTreePage<>(lastPage++, leaf, maxDegree, keySerializer, valueSerializer);
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
        writeHeader(dir);
        writePages(dir);
        cleanPages(dir);
    }

    private int[] readHeader(File dir) {
        return new int[0];
    }

    private void writeHeader(File dir) throws IOException {
        File file = new File(dir, "index.jmb.dat");
        if (file.exists() && !file.delete()) {
            throw new IOException("Could not write file " + file.getAbsolutePath());
        }
        if (file.createNewFile()) {
            throw new IOException("Could not write file " + file.getAbsolutePath());
        }
        int[] blocks = new int[3];
        blocks[0] = root;
        blocks[1] = maxDegree;
        blocks[2] = lastPage;
        BlockStorage blockStorage = new FileBlockStorage(INDEX_BLOCK_SIZE, new RandomAccessFile(file, "rw"));
        for (int block : blocks) {
            int index = blockStorage.createBlock();
            ByteBuffer buffer = ByteBuffer.allocate(INDEX_BLOCK_SIZE);
            buffer.putInt(block);
            blockStorage.write(index, buffer);
        }
    }

    private void readPages(File dir, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        for (int i = 0; i < lastPage; i++) {
            try {
                FileBTreePage<K, V> page = new FileBTreePage<>(i, dir, keySerializer, valueSerializer);
                map.put(i, page);
            } catch (IOException ignored) {
            }
        }
    }

    private void writePages(File dir) throws IOException {
        Collection<FileBTreePage<K, V>> pages = map.values();
        for (FileBTreePage<K, V> page : pages) {
            page.fsync(dir);
        }
    }

    private void cleanPages(File dir) {
        List<Integer> ids = new ArrayList<>();
        map.forEach((key, value) -> {
            if (value == null) {
                ids.add(key);
            }
        });
        File[] files = dir.listFiles((fileDir, name) -> {
            for (int id : ids) {
                if (name.endsWith(String.format("-%d.jmb.dat", id))) {
                    return true;
                }
            }
            return false;
        });
        if (files != null) {
            for (File file : files) {
                //noinspection ResultOfMethodCallIgnored
                file.delete();
            }
        }
    }
}

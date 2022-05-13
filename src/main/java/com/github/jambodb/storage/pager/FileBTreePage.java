package com.github.jambodb.storage.pager;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.btrees.BTreePage;
import com.github.jambodb.storage.btrees.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileBTreePage<K, V> implements BTreePage<K, V> {
    private static final short FLAG_IS_LEAF = 1;

    private static final short FLAG_IS_FRAG = 2;

    private static final int FLAGS_POS = 0;

    private static final int SIZE_POS = 2;

    private static final int AD_POINTER_POS = 4;

    private static final int USED_BYTES_POS = 6;

    private static final int ELEMENTS_POS = 8;

    public static <K, V> FileBTreePage<K, V> create(BlockStorage storage, boolean isLeaf, Serializer<K> keySer, Serializer<V> valueSer) throws IOException {
        return new FileBTreePage<>(storage, isLeaf, keySer, valueSer);
    }

    public static <K, V> FileBTreePage<K, V> load(BlockStorage storage, int id, Serializer<K> keySer, Serializer<V> valueSer) throws IOException {
        return new FileBTreePage<>(storage, id, keySer, valueSer);
    }

    private final int id;

    private final ByteBuffer buffer;

    private final BlockStorage storage;

    private final Serializer<K> keySer;

    private final Serializer<V> valueSer;

    private boolean leaf;

    private boolean modified;

    private Map<Short, ByteBuffer> overflowMap;

    private FileBTreePage(BlockStorage storage, int id, Serializer<K> keySer, Serializer<V> valueSer) throws IOException {
        this.id = id;
        this.storage = storage;
        this.keySer = keySer;
        this.valueSer = valueSer;

        this.buffer = ByteBuffer.allocate(BlockStorage.BLOCK_SIZE);
        this.storage.read(id, this.buffer);

        leaf = (flags() & FLAG_IS_LEAF) != 0;
    }

    private FileBTreePage(BlockStorage storage, boolean isLeaf, Serializer<K> keySer, Serializer<V> valueSer) throws IOException {
        this.storage = storage;
        this.keySer = keySer;
        this.valueSer = valueSer;

        this.id = storage.increase();
        this.buffer = ByteBuffer.allocate(BlockStorage.BLOCK_SIZE);
        this.leaf = isLeaf;

        if(isLeaf) {
            flags(FLAG_IS_LEAF);
        }
        adPointer((short)BlockStorage.BLOCK_SIZE);
        usedBytes((short)0);
        size(0);
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public int size() {
        return buffer.getShort(SIZE_POS);
    }

    @Override
    public void size(int value) {
        int prevSize = size();

        buffer.putShort(SIZE_POS, (short) value);

        if(headerSize() > adPointer()) {
            buffer.putShort(SIZE_POS, (short) prevSize);
            defragment();
            buffer.putShort(SIZE_POS, (short) value);
        }

        for(int i = prevSize; i <= value; i++) {
            keyPos(i, (short)0);
            valuePos(i, (short)0);
            child(i+1, 0);
        }
    }

    @Override
    public boolean isLeaf() {
        return leaf;
    }

    public boolean isModified() {
        return modified;
    }

    public void save() throws IOException {
        if(isFragmented() && hasOverflow()) {
            defragment();
        }
        if(hasOverflow()) {
            throw new IOException("page overflow");
        }
        buffer.position(0);
        storage.write(id, buffer);
        modified = false;
    }

    @Override
    public boolean isFull() {
        return hasOverflow();
    }

    @Override
    public boolean isHalf() {
        return usedBytes() < (bodySize() / 3);
    }

    @Override
    public boolean canBorrow() {
        return size() > 1 && usedBytes() > (bodySize() / 2);
    }

    @Override
    public K key(int index) {
        return readData(keyPos(index), keySer);
    }

    @Override
    public void key(int index, K data) {
        removeData(keyPos(index), keySer);
        keyPos(index, appendData(data, keySer));
        modified = true;
    }

    @Override
    public V value(int index) {
        return readData(valuePos(index), valueSer);
    }

    @Override
    public void value(int index, V data) {
        int pos = valuePos(index);
        removeData(pos, valueSer);
        valuePos(index, appendData(data, valueSer));
        modified = true;
    }

    @Override
    public int child(int index) {
        return buffer.getInt(elementPos(index));
    }

    @Override
    public void child(int index, int id) {
        buffer.putInt(elementPos(index), id);
        modified = true;
    }

    private short flags() {
        return buffer.getShort(FLAGS_POS);
    }

    private void flags(short value) {
        buffer.putShort(FLAGS_POS, value);
    }

    private short adPointer() {
        return buffer.getShort(AD_POINTER_POS);
    }

    private void adPointer(short value) {
        buffer.putShort(AD_POINTER_POS, value);
    }

    private short usedBytes() {
        return buffer.getShort(USED_BYTES_POS);
    }

    private void usedBytes(short value) {
        buffer.putShort(USED_BYTES_POS, value);
    }

    private int keyPos(int index) {
        int pos = leaf ? 0 : 4;
        return buffer.getShort(elementPos(index) + pos);
    }

    private void keyPos(int index, short value) {
        int pos = leaf ? 0 : 4;
        buffer.putShort(elementPos(index) + pos, value);
    }

    private int valuePos(int index) {
        int relPos = leaf ? 2 : 6;
        int bytePos = elementPos(index) + relPos;
        return buffer.getShort(bytePos);
    }

    private void valuePos(int index, short value) {
        int relPos = leaf ? 2 : 6;
        int buffPos = elementPos(index) + relPos;
        buffer.putShort(buffPos, value);
    }

    private <T> short appendData(T value, Serializer<T> ser) {
        int byteCount = ser.size(value);
        if(byteCount > BlockStorage.BLOCK_SIZE / 4) {
            throw new IllegalArgumentException("invalid data size");
        }
        int position = adPointer() - byteCount;
        if(position <= headerSize()) {
            return overflow(value, ser);
        }

        buffer.position(position);
        ser.write(buffer, value);
        adPointer((short) position);
        usedBytes((short) (usedBytes() + byteCount));
        return (short) position;
    }

    private <T> T readData(int position, Serializer<T> ser) {
        if(position == 0) {
            throw new IllegalArgumentException("invalid position");
        }
        if(position < 0) {
            ByteBuffer buffer = overflowMap.get((short) position);
            buffer.position(0);
            return ser.read(buffer);
        }
        else {
            buffer.position(position);
            return ser.read(buffer);
        }
    }

    private <T> void removeData(int position, Serializer<T> ser) {
        if(position == 0) {
            return;
        }

        if(position < 0) {
            overflowMap.remove((short) position);
        }
        else {
            buffer.position(position);
            int bytes = ser.size(buffer);
            usedBytes((short) (usedBytes() - bytes));
            setFragmented(true);
        }
    }

    private void defragment() {
        System.out.println("defrag");
        int size = size();
        List<K> keys = new ArrayList<>(size);
        List<V> values = new ArrayList<>(size);
        for(int i = 0; i < size; i++) {
            keys.add(readData(keyPos(i), keySer));
            values.add(readData(valuePos(i), valueSer));
        }
        usedBytes((short) 0);
        adPointer((short) BlockStorage.BLOCK_SIZE);
        overflowMap = null;
        for(int i = 0; i < size; i++) {
            keyPos(i, appendData(keys.get(i), keySer));
            valuePos(i, appendData(values.get(i), valueSer));
        }
        setFragmented(false);
    }

    private <T> short overflow(T value, Serializer<T> ser) {
        if(overflowMap == null) {
            overflowMap = new HashMap<>();
        }

        ByteBuffer bb = ByteBuffer.allocate(ser.size(value));
        ser.write(bb, value);
        var key = overflowMap.keySet()
                .stream()
                .min(Short::compareTo)
                .orElse((short)0);
        key--;
        overflowMap.put(key,  bb);
        return key;
    }

    private int elementPos(int index) {
        int elementSize = leaf ? 4 : 8;
        return ELEMENTS_POS + (index * elementSize);
    }

    private int headerSize() {
        int size = size();
        if(leaf) {
            return ELEMENTS_POS + (size * 4);
        }
        return ELEMENTS_POS + (size * 8) + 4;
    }

    private int bodySize() {
        return BlockStorage.BLOCK_SIZE - headerSize();
    }

    private boolean hasOverflow() {
        return overflowMap != null;
    }

    private void setFragmented(boolean isFrag) {
        if(isFrag) {
            flags((short) (flags() | FLAG_IS_FRAG));
        }
        else {
            flags((short) (flags() & ~FLAG_IS_FRAG));
        }
    }

    private boolean isFragmented() {
        return (flags() & FLAG_IS_FRAG) != 0;
    }
}
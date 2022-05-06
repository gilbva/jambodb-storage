package com.github.jambodb.storage.pager;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.btrees.BTreePage;
import com.github.jambodb.storage.btrees.Serializer;
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

    private static final int DATA_POINTER_POS = 4;

    private static final int USED_BYTES_POS = 6;

    private static final int ELEMENTS_POS = 8;

    private final int id;

    private final ByteBuffer buffer;

    private final BlockStorage storage;

    private final Serializer<K> keySer;

    private final Serializer<V> valueSer;

    private boolean isLeaf;

    private Map<Short, ByteBuffer> overflowMap;

    public FileBTreePage(int id, ByteBuffer buffer, BlockStorage storage, Serializer<K> keySer, Serializer<V> valueSer) {
        this.id = id;
        this.buffer = buffer;
        this.storage = storage;
        this.keySer = keySer;
        this.valueSer = valueSer;

        short flags = readFlags();
        isLeaf = (flags & FLAG_IS_LEAF) != 0;
    }

    public FileBTreePage(int id, ByteBuffer buffer, BlockStorage storage, Serializer<K> keySer, Serializer<V> valueSer, boolean isLeaf) {
        this.id = id;
        this.buffer = buffer;
        this.storage = storage;
        this.keySer = keySer;
        this.valueSer = valueSer;

        if(isLeaf) {
            writeFlags(FLAG_IS_LEAF);
        }
        writeUsedBytes((short) (BlockStorage.BLOCK_SIZE - lastElementPos() - 1));
        writeSize((short) 0);
        writeDataPointer((short) BlockStorage.BLOCK_SIZE);
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public int size() {
        return readSize();
    }

    @Override
    public void size(int value) {
        writeSize((short) value);
    }

    @Override
    public boolean isLeaf() {
        return readFlags() == (short) 0;
    }

    @Override
    public boolean isFull() {
        return hasOverflow();
    }

    @Override
    public boolean isHalf() {
        int dataBytes = BlockStorage.BLOCK_SIZE - lastElementPos() - 1;
        return readUsedBytes() < dataBytes;
    }

    @Override
    public boolean canBorrow() {
        return false;
    }

    @Override
    public K key(int index) {
        return readData(readKey(index), keySer);
    }

    @Override
    public void key(int index, K data) {
        removeData(readKey(index), keySer);
        writeKey(index, appendData(data, keySer));
    }

    @Override
    public V value(int index) {
        return readData(readValue(index), valueSer);
    }

    @Override
    public void value(int index, V data) {
        removeData(readValue(index), valueSer);
        writeValue(index, appendData(data, valueSer));
    }

    @Override
    public int child(int index) {
        return readChild(index);
    }

    @Override
    public void child(int index, int id) {
        writeChild(index, id);
    }


    public short readFlags() {
        return buffer.getShort(FLAGS_POS);
    }

    public void writeFlags(short value) {
        buffer.putShort(FLAGS_POS, value);
    }

    public short readSize() {
        return buffer.getShort(SIZE_POS);
    }

    public void writeSize(short value) {
        buffer.putShort(SIZE_POS, value);
    }

    public short readDataPointer() {
        return buffer.getShort(DATA_POINTER_POS);
    }

    public void writeDataPointer(short value) {
        buffer.putShort(DATA_POINTER_POS, value);
    }

    public short readUsedBytes() {
        return buffer.getShort(USED_BYTES_POS);
    }

    public void writeUsedBytes(short value) {
        buffer.putShort(USED_BYTES_POS, value);
    }

    public int readChild(int index) {
        return buffer.getInt(elementPos(index));
    }

    public void writeChild(int index, int value) {
        buffer.putInt(elementPos(index), value);
    }

    public int readKey(int index) {
        int pos = isLeaf ? 4 : 0;
        return buffer.getShort(elementPos(index) + pos);
    }

    public void writeKey(int index, short value) {
        int pos = isLeaf ? 4 : 0;
        buffer.putShort(elementPos(index) + pos, value);
    }

    public int readValue(int index) {
        int pos = isLeaf ? 6 : 2;
        return buffer.getShort(elementPos(index) + pos);
    }

    public void writeValue(int index, short value) {
        int pos = isLeaf ? 6 : 2;
        buffer.putShort(elementPos(index) + pos, value);
    }

    public <T> short appendData(T value, Serializer<T> ser) {
        int lastElementPos = lastElementPos();
        int bytes = ser.size(value);
        int position = readDataPointer() - bytes;
        if(position < lastElementPos) {
            return overflow(value, ser);
        }

        buffer.position(position);
        ser.write(buffer, value);
        writeDataPointer((short) position);
        writeUsedBytes((short) (readUsedBytes() - bytes));
        return (short) position;
    }

    public void defrag() {
        int bufferSize = BlockStorage.BLOCK_SIZE - lastElementPos();
        ByteBuffer tempBuffer = ByteBuffer.allocate(bufferSize);
        tempBuffer.position(bufferSize - 1);
        int size = readSize();
        List<K> keys = new ArrayList<>(size);
        List<V> values = new ArrayList<>(size);
        for(int i = 0; i < size; i++) {
            keys.add(readData(readKey(i), keySer));
            values.add(readData(readValue(i), valueSer));
        }
        writeDataPointer((short) BlockStorage.BLOCK_SIZE);
        overflowMap = null;
        for(int i = 0; i < size; i++) {
            writeKey(i, appendData(keys.get(i), keySer));
            writeValue(i, appendData(values.get(i), valueSer));
        }
    }

    public <T> T readData(int position, Serializer<T> ser) {
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

    public <T> void removeData(int position, Serializer<T> ser) {
        buffer.position(position);
        int bytes = ser.size(buffer);
        writeUsedBytes((short) (readUsedBytes() + bytes));
        writeFlags((short) (readFlags() | FLAG_IS_FRAG));
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

    public int elementPos(int index) {
        int elementSize = isLeaf ? 8 : 4;
        return ELEMENTS_POS + (index * elementSize);
    }

    public int lastElementPos() {
        int size = readSize();
        if(isLeaf) {
            return ELEMENTS_POS + size * 4;
        }
        return ELEMENTS_POS + (size * 8) + 4;
    }

    public boolean hasOverflow() {
        return overflowMap != null;
    }

    public boolean isFrag() {
        return (readFlags() & FLAG_IS_FRAG) != 0;
    }
}
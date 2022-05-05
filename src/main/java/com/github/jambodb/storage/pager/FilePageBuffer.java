package com.github.jambodb.storage.pager;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.btrees.Serializer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class FilePageBuffer {
    private static final short FLAG_IS_LEAF = 1;

    private static final int FLAGS_POS = 0;

    private static final int SIZE_POS = 2;

    private static final int DATA_POINTER_POS = 4;

    private static final int ELEMENTS_POS = 6;

    private ByteBuffer buffer;

    private boolean isLeaf;

    private Map<Short, byte[]> overflowMap;

    public FilePageBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
        short flags = readFlags();
        isLeaf = (flags & FLAG_IS_LEAF) != 0;
    }

    public FilePageBuffer(ByteBuffer buffer, boolean isLeaf) {
        this.buffer = buffer;
        this.isLeaf = isLeaf;
        if(isLeaf) {
            writeFlags(FLAG_IS_LEAF);
        }
        writeDataPointer((short) BlockStorage.BLOCK_SIZE);
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

    public <T> short appendValue(T value, Serializer<T> ser) {
        int lastElementPos = lastElementPos();
        int position = readDataPointer() - ser.size(value);
        if(position < lastElementPos) {
            return overflow(value, ser);
        }
        else {
            buffer.position(position);
            ser.write(buffer, value);
            return (short) position;
        }
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
        overflowMap.put(key,  bb.array());
        return key;
    }

    public int elementPos(int index) {
        int elementSize = isLeaf ? 8 : 4;
        return ELEMENTS_POS + (index * elementSize);
    }

    public int lastElementPos() {
        int size = readSize();
        if(isLeaf) {
            return size * 4;
        }
        return (size * 8) + 4;
    }
}

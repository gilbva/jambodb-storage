package com.github.jambodb.storage.pager;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.btrees.Serializer;

import java.nio.ByteBuffer;

public class FilePageBuffer {
    private static final int POINTER_SIZE = 2;

    private static final short FLAG_IS_LEAF = 1;

    private static final int FLAGS_POS = 0;

    private static final int SIZE_POS = 2;

    private static final int DATA_POINTER_POS = 4;

    private static final int ELEMENTS_POS = 6;

    private ByteBuffer buffer;

    private int elementSize;

    public FilePageBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
        this.elementSize = 3;
        short flags = readFlags();
        if((flags & FLAG_IS_LEAF) != 0) {
            this.elementSize = 2;
        }
    }

    public FilePageBuffer(ByteBuffer buffer, boolean isLeaf) {
        this.buffer = buffer;
        short flags = readFlags();
        if(isLeaf) {
            writeFlags((short) (flags | FLAG_IS_LEAF));
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

    public short readElementPointer(int element, int subElement) {
        return buffer.getShort(elementPointerPos(element, subElement));
    }

    public void writeElementPointer(int element, int subElement, short value) {
        buffer.putShort(elementPointerPos(element, subElement), value);
    }

    public <T> short appendValue(T value, Serializer<T> ser) {

    }

    public int elementPointerPos(int element, int subElement) {
        return ELEMENTS_POS + (element * elementSize * POINTER_SIZE) + (subElement * POINTER_SIZE);
    }
}

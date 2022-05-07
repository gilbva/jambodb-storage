package com.github.jambodb.storage.types;

import com.github.jambodb.storage.btrees.Serializer;

import java.nio.ByteBuffer;

public class ShortSerializer implements Serializer<Short> {
    public static final ShortSerializer INSTANCE = new ShortSerializer();

    @Override
    public int size(ByteBuffer buffer) {
        return 2;
    }

    @Override
    public int size(Short value) {
        return 2;
    }

    @Override
    public Short read(ByteBuffer buffer) {
        return buffer.getShort();
    }

    @Override
    public void write(ByteBuffer buffer, Short value) {
        buffer.putShort(value);
    }
}

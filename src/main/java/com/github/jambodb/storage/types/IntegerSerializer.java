package com.github.jambodb.storage.types;

import com.github.jambodb.storage.btrees.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class IntegerSerializer implements Serializer<Integer> {
    public static final IntegerSerializer INSTANCE = new IntegerSerializer();

    @Override
    public int size(ByteBuffer buffer) {
        return 4;
    }

    @Override
    public int size(Integer value) {
        return 4;
    }

    @Override
    public Integer read(ByteBuffer buffer) {
        return buffer.getInt();
    }

    @Override
    public void write(ByteBuffer buffer, Integer value) {
        buffer.putInt(value);
    }
}

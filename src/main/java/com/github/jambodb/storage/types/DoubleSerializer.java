package com.github.jambodb.storage.types;

import com.github.jambodb.storage.btrees.Serializer;

import java.nio.ByteBuffer;

public class DoubleSerializer implements Serializer<Double> {
    public static final DoubleSerializer INSTANCE = new DoubleSerializer();

    @Override
    public int size(ByteBuffer buffer) {
        return 4;
    }

    @Override
    public int size(Double value) {
        return 4;
    }

    @Override
    public Double read(ByteBuffer buffer) {
        return buffer.getDouble();
    }

    @Override
    public void write(ByteBuffer buffer, Double value) {
        buffer.putDouble(value);
    }
}

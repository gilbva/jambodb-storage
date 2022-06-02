package com.github.jambodb.storage.types;

import com.github.jambodb.storage.btrees.Serializer;

import java.nio.ByteBuffer;

/**
 * Serializer for java.lang.Float.
 */
public class FloatSerializer implements Serializer<Float> {
    public static final FloatSerializer INSTANCE = new FloatSerializer();

    @Override
    public int size(ByteBuffer buffer) {
        return 4;
    }

    @Override
    public int size(Float value) {
        return 4;
    }

    @Override
    public Float read(ByteBuffer buffer) {
        return buffer.getFloat();
    }

    @Override
    public void write(ByteBuffer buffer, Float value) {
        buffer.putFloat(value);
    }
}

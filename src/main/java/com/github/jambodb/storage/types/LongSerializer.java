package com.github.jambodb.storage.types;

import com.github.jambodb.storage.btrees.Serializer;

import java.nio.ByteBuffer;

/**
 * Serializer for java.lang.Long.
 */
public class LongSerializer implements Serializer<Long> {
    public static final LongSerializer INSTANCE = new LongSerializer();

    @Override
    public int size(ByteBuffer buffer) {
        return 8;
    }

    @Override
    public int size(Long value) {
        return 8;
    }

    @Override
    public Long read(ByteBuffer buffer) {
        return buffer.getLong();
    }

    @Override
    public void write(ByteBuffer buffer, Long value) {
        buffer.putLong(value);
    }
}

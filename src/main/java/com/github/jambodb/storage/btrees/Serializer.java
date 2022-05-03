package com.github.jambodb.storage.btrees;

import java.nio.ByteBuffer;

/**
 * This interface defines parsing/serializing functions for a particular type of values from/to bytes array.
 *
 * @param <T> the type this serializer manages.
 */
public interface Serializer<T> {
    int size(T value);

    T read(ByteBuffer buffer);

    void write(ByteBuffer buffer, T value);
}

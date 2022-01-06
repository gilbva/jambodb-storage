package com.github.jambodb.storage.btrees;

import java.nio.ByteBuffer;

/**
 * This class allows to read/write a particular type of values to the/from the given buffer.
 *
 * @param <T> the type this serializer manages.
 */
public interface Serializer<T> {
    /**
     * Read the next value from the buffer.
     *
     * @param buffer the buffer to read the value from.
     * @return the read value from the buffer.
     */
    T read(ByteBuffer buffer);

    /**
     * Writes the given value to the buffer.
     *
     * @param value the value to write.
     * @param buffer the buffer to write to.
     */
    void write(T value, ByteBuffer buffer);
}

package me.gilbva.jambodb.storage.btrees;

import java.nio.ByteBuffer;

/**
 * This interface defines parsing/serializing functions for a particular type of values from/to bytes array.
 *
 * @param <T> the type this serializer manages.
 */
public interface Serializer<T> {
    /**
     * gets the size in bytes of the value stored at the current position in the given buffer.
     *
     * @param buffer the buffer to read the value from.
     * @return an integer representing the amount of bytes the value has.
     */
    int size(ByteBuffer buffer);

    /**
     * gets the size in bytes of the given value.
     *
     * @param value the size of the given value
     * @return an integer representing the amount of bytes needed to store the value.
     */
    int size(T value);

    /**
     * reads the value at the current position in the buffer.
     *
     * @param buffer the buffer to read the value from.
     * @return the value at the current position of the buffer.
     */
    T read(ByteBuffer buffer);

    /**
     * writes the given value to the buffer at the current position.
     *
     * @param buffer the buffer to write the value to.
     * @param value the value to write to the buffer.
     */
    void write(ByteBuffer buffer, T value);
}

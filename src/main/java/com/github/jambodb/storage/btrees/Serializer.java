package com.github.jambodb.storage.btrees;

/**
 * This interface defines parsing/serializing functions for a particular type of values from/to bytes array.
 *
 * @param <T> the type this serializer manages.
 */
public interface Serializer<T> {
    /**
     * Read the next value from the buffer.
     *
     * @param bytes the bytes array to read the value from.
     * @return the read value from the buffer.
     */
    T parse(byte[] bytes);

    /**
     * Writes the given value to the buffer.
     *
     * @param value the value to write.
     * @return value as bytes array.
     */
    byte[] serialize(T value);
}

package com.github.jambodb.storage.btrees;

import java.nio.ByteBuffer;

public interface Serializer<T> {
    T read(ByteBuffer buffer);

    void write(T value, ByteBuffer buffer);
}

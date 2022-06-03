package me.gilbva.jambodb.storage.types;

import me.gilbva.jambodb.storage.btrees.Serializer;

import java.nio.ByteBuffer;

/**
 * Serializer for java.lang.Boolean.
 */
public class BooleanSerializer implements Serializer<Boolean> {
    public static final BooleanSerializer INSTANCE = new BooleanSerializer();

    @Override
    public int size(ByteBuffer buffer) {
        return 1;
    }

    @Override
    public int size(Boolean value) {
        return 1;
    }

    @Override
    public Boolean read(ByteBuffer buffer) {
        return buffer.get() == 1;
    }

    @Override
    public void write(ByteBuffer buffer, Boolean value) {
        buffer.put(value ? (byte) 1 : (byte) 0 );
    }
}

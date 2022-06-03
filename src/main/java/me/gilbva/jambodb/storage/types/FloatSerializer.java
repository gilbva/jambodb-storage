package me.gilbva.jambodb.storage.types;

import me.gilbva.jambodb.storage.btrees.Serializer;

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

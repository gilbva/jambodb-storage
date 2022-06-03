package me.gilbva.jambodb.storage.types;

import me.gilbva.jambodb.storage.btrees.Serializer;

import java.nio.ByteBuffer;

/**
 * Serializer for java.lang.Double.
 */
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

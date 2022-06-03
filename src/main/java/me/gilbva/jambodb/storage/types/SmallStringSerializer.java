package me.gilbva.jambodb.storage.types;

import me.gilbva.jambodb.storage.btrees.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Serializer for java.lang.String.
 */
public class SmallStringSerializer implements Serializer<String> {
    public static final SmallStringSerializer INSTANCE = new SmallStringSerializer();

    @Override
    public int size(ByteBuffer buffer) {
        return buffer.getShort() + 2;
    }

    @Override
    public int size(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length + 2;
    }

    @Override
    public String read(ByteBuffer buffer) {
        int size = buffer.getShort();
        if(size < 0) {
            throw new IllegalArgumentException("invalid length");
        }
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public void write(ByteBuffer buffer, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        short len = (short) (bytes.length);
        if(len < 0) {
            throw new IllegalArgumentException("invalid length");
        }
        buffer.putShort(len);
        buffer.put(bytes);
    }
}

package com.github.jambodb.storage.types;

import com.github.jambodb.storage.btrees.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

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
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public void write(ByteBuffer buffer, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        buffer.putShort((short) (bytes.length));
        buffer.put(bytes);
    }
}

package com.github.jambodb.storage.btrees;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StringSerializer implements Serializer<String> {
    @Override
    public String read(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public void write(String value, ByteBuffer buffer) {
        if (value != null) {
            byte[] bytes = StandardCharsets.UTF_8.encode(value).array();
            buffer.put(bytes);
        }
    }
}

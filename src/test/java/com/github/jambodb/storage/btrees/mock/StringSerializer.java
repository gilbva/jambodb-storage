package com.github.jambodb.storage.btrees.mock;

import com.github.jambodb.storage.btrees.Serializer;
import java.nio.charset.StandardCharsets;

public class StringSerializer implements Serializer<String> {
    @Override
    public String parse(byte[] bytes) {
        if (bytes != null) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return null;
    }

    @Override
    public byte[] serialize(String string) {
        if (string != null) {
            return StandardCharsets.UTF_8.encode(string).array();
        }
        return null;
    }
}

package com.github.jambodb.storage.btrees.mock;

import com.github.jambodb.storage.btrees.Serializer;
import java.nio.charset.StandardCharsets;

public class MockObjectSerializer implements Serializer<MockObject> {
    private static final String sep = "<>";

    @Override
    public MockObject parse(byte[] bytes) {
        String string = new String(bytes, StandardCharsets.UTF_8);
        String[] parts = string.split(sep);
        if (parts.length == 3) {
            MockObject object = new MockObject();
            object.setStringValue(parts[0].trim());
            object.setBoolValue("1".equals(parts[1].trim()));
            object.setIntValue(Integer.parseInt(parts[2].trim()));
            return object;
        }
        return null;
    }

    @Override
    public byte[] serialize(MockObject value) {
        if (value != null) {
            String string = String.format("%s%s%d%s%d",
                value.getStringValue(), sep, value.isBoolValue() ? 1 : 0, sep, value.getIntValue());
            return StandardCharsets.UTF_8.encode(string).array();
        }
        return null;
    }
}

package com.github.jambodb.storage.btrees;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MockObjectSerializer implements Serializer<MockObject> {
    @Override
    public MockObject read(ByteBuffer buffer) {
        String string = new String(buffer.array(), StandardCharsets.UTF_8);
        String[] parts = string.split("\\|");
        if (parts.length == 3) {
            MockObject object = new MockObject();
            object.setStringValue(parts[0]);
            object.setBoolValue("1".equals(parts[1]));
            object.setIntValue(Integer.parseInt(parts[2]));
            return object;
        }
        return null;
    }

    @Override
    public void write(MockObject value, ByteBuffer buffer) {
        if (value != null) {
            String string = String.format("%s|%d|%d",
                value.getStringValue(), value.isBoolValue() ? 1 : 0, value.getIntValue());
            byte[] bytes = StandardCharsets.UTF_8.encode(string).array();
            buffer.put(bytes);
        }
    }
}

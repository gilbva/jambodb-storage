package com.github.jambodb.storage.btrees;

import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FileBTreeTests {
    private static Serializer<String> stringSerializer;
    private static Serializer<MockObject> mockSerializer;

    @BeforeAll
    public static void beforeAll() {
        stringSerializer = new StringSerializer();
        mockSerializer = new MockObjectSerializer();
    }

    @Test
    public void testFsync() throws IOException {
        MockObject mockObject = new MockObject();
        mockObject.setStringValue("string value");
        mockObject.setBoolValue(true);
        mockObject.setIntValue(10);

        FileBTreePage<String, MockObject> btree
            = new FileBTreePage<>(0, true, 2, stringSerializer, mockSerializer, 8 * 1024);
        btree.key(0, mockObject.getStringValue());
        btree.value(0, mockObject);
        File dir = new File(System.getProperty("java.io.tmpdir"), "jambodb.btree-test");
        //noinspection ResultOfMethodCallIgnored
        dir.mkdir();
        btree.fsync(dir);

        FileBTreePage<String, MockObject> readBtree
            = new FileBTreePage<>(0, dir, stringSerializer, mockSerializer);
        assertEquals(mockObject.getStringValue(), readBtree.key(0).trim());
        assertEquals(mockObject, readBtree.value(0));
    }
}

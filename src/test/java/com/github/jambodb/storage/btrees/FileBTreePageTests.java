package com.github.jambodb.storage.btrees;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.blocks.FileBlockStorage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.github.jambodb.storage.btrees.mock.MockObject;
import com.github.jambodb.storage.btrees.mock.MockObjectSerializer;
import com.github.jambodb.storage.btrees.mock.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FileBTreePageTests {
    private static final int BLOCK_SIZE = 8 * 1024;

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
            = new FileBTreePage<>(0, true, 2, stringSerializer, mockSerializer);
        btree.key(0, mockObject.getStringValue());
        btree.value(0, mockObject);
        Path path = Files.createTempFile("jambodb.btree", "-test");
        Files.deleteIfExists(path);
        try (BlockStorage blockStorage = FileBlockStorage.writeable(BLOCK_SIZE, path)) {
            btree.fsync(blockStorage);
        }

        try (BlockStorage blockStorage = FileBlockStorage.readable(path)) {
            FileBTreePage<String, MockObject> readBtree
                = FileBTreePage.read(0, blockStorage, stringSerializer, mockSerializer);
            assertEquals(mockObject.getStringValue(), readBtree.key(0).trim());
            assertEquals(mockObject, readBtree.value(0));
        }
    }
}

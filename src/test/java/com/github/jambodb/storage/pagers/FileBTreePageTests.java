package com.github.jambodb.storage.pagers;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.blocks.FileBlockStorage;
import com.github.jambodb.storage.btrees.Serializer;
import com.github.jambodb.storage.pagers.FileBTreePage;
import com.github.jambodb.storage.btrees.mock.MockObject;
import com.github.jambodb.storage.btrees.mock.MockObjectSerializer;
import com.github.jambodb.storage.btrees.mock.StringSerializer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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

        FileBTreePage<String, MockObject> page
            = new FileBTreePage<>(0, true, 2, stringSerializer, mockSerializer);
        page.size(1);
        page.key(0, mockObject.getStringValue());
        page.value(0, mockObject);
        Path path = Files.createTempFile("jambodb.btree", ".test");
        Files.deleteIfExists(path);
        try (BlockStorage blockStorage = FileBlockStorage.writeable(BLOCK_SIZE, path)) {
            blockStorage.blockCount(2);
            page.fsync(blockStorage);
        }

        try (BlockStorage blockStorage = FileBlockStorage.readable(path)) {
            FileBTreePage<String, MockObject> readPage
                = FileBTreePage.read(0, blockStorage, stringSerializer, mockSerializer);
            assertNotNull(readPage);
            assertEquals(page.size(), readPage.size());
            assertEquals(mockObject.getStringValue(), readPage.key(0).trim());
            assertEquals(mockObject, readPage.value(0));
        }
    }
}

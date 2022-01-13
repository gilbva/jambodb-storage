package com.github.jambodb.storage.btrees;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class FilePagerTests {
    private static Serializer<String> stringSerializer;
    private static Serializer<MockObject> mockSerializer;

    @BeforeAll
    public static void beforeAll() {
        stringSerializer = new StringSerializer();
        mockSerializer = new MockObjectSerializer();
    }

    @Test
    public void testFsync() throws IOException {
        Path path = Files.createTempDirectory("jambodb.btree-pager-test");
        FilePager<String, MockObject> pager
            = new FilePager<>(2, path, stringSerializer, mockSerializer, 8 * 1024);
        for (int i = 0; i < 10; i++) {
            MockObject mockObject = createMockObject(i);
            FileBTreePage<String, MockObject> page = pager.create(i % 2 != 0);
            page.key(0, mockObject.getStringValue());
            page.value(0, mockObject);
        }
        pager.fsync();

        FilePager<String, MockObject> readPager = new FilePager<>(path, stringSerializer, mockSerializer);
        assertEquals(pager.blockSize(), readPager.blockSize());
        assertEquals(pager.lastPage(), readPager.lastPage());
        for (int i = 2; i < 6; i++) {
            FileBTreePage<String, MockObject> page = readPager.page(i);
            assertNotNull(page);
            if (i % 2 != 0) {
                assertTrue(page.isLeaf());
            } else {
                assertFalse(page.isLeaf());
            }
            assertEquals("value" + i, page.key(0).trim());
            MockObject mockObject = createMockObject(i);
            assertEquals(mockObject, page.value(0));
        }
    }

    private static MockObject createMockObject(int index) {
        MockObject mockObject = new MockObject();
        mockObject.setStringValue("value" + index);
        mockObject.setBoolValue(index % 2 == 0);
        mockObject.setIntValue(index);
        return mockObject;
    }
}

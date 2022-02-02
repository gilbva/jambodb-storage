package com.github.jambodb.storage.pagers;

import com.github.jambodb.storage.btrees.Serializer;
import com.github.jambodb.storage.pagers.FileBTreePage;
import com.github.jambodb.storage.pagers.FilePager;
import com.github.jambodb.storage.btrees.mock.MockObject;
import com.github.jambodb.storage.btrees.mock.MockObjectSerializer;
import com.github.jambodb.storage.btrees.mock.StringSerializer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
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
        Path path = Files.createTempFile("jambodb.btree-pager", ".test");
        FilePager<String, MockObject> pager
            = new FilePager<>(2, path, stringSerializer, mockSerializer);
        int totalPages = 10;
        for (int i = 1; i < totalPages; i++) {
            MockObject mockObject = createMockObject(i);
            FileBTreePage<String, MockObject> page = pager.create(i % 2 != 0);
            page.size(1);
            page.key(0, mockObject.getStringValue());
            page.value(0, mockObject);
        }
        List<Integer> deleted = Arrays.asList(3, 7, 8);
        for (int page : deleted) {
            pager.remove(page);
        }
        pager.fsync();

        FilePager<String, MockObject> readPager = new FilePager<>(path, stringSerializer, mockSerializer);
        assertEquals(pager.lastPage(), readPager.lastPage());
        for (int i = 1; i < totalPages; i++) {
            FileBTreePage<String, MockObject> page = readPager.page(i);
            if (deleted.contains(i)) {
                assertNull(page);
            } else {
                assertNotNull(page);
                assertFalse(page.dirty());
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
    }

    private static MockObject createMockObject(int index) {
        MockObject mockObject = new MockObject();
        mockObject.setStringValue("value" + index);
        mockObject.setBoolValue(index % 2 == 0);
        mockObject.setIntValue(index);
        return mockObject;
    }
}

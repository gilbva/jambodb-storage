package me.gilbva.jambodb.storage.pager;

import me.gilbva.jambodb.storage.blocks.BlockStorage;
import me.gilbva.jambodb.storage.types.IntegerSerializer;
import me.gilbva.jambodb.storage.types.SmallStringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class FileBTreePageTest {

    @TestFactory
    public List<DynamicTest> testAll() {
        List<DynamicTest> lst = new ArrayList<>();

        lst.add(DynamicTest.dynamicTest("testing FileBTreePage leaf", () -> testPage(true)));
        lst.add(DynamicTest.dynamicTest("testing FileBTreePage non-leaf", () -> testPage(false)));

        return lst;
    }

    public void testPage(boolean leaf) throws IOException {
        var tmpFile = Files.createTempFile("test", "jambodb");
        var storage = BlockStorage.create(tmpFile);
        var page = FileBTreePage.create(storage, leaf, SmallStringSerializer.INSTANCE, IntegerSerializer.INSTANCE);
        List<String> lst = new ArrayList<>();
        short usedBytes = 0;
        for (int i = 0; !page.isFull(); i++) {
            lst.add(UUID.randomUUID().toString());

            page.size(i+1);
            Assertions.assertEquals(i+1, page.size());
            Assertions.assertTrue(page.isModified());

            page.key(i, lst.get(i));
            usedBytes += lst.get(i).getBytes(StandardCharsets.UTF_8).length + 2;
            Assertions.assertEquals(usedBytes, page.usedBytes());
            Assertions.assertEquals(lst.get(i), page.key(i));
            Assertions.assertTrue(page.isModified());

            page.value(i, i*2);
            usedBytes += 4;
            Assertions.assertEquals(usedBytes, page.usedBytes());
            Assertions.assertEquals(i*2, page.value(i));
            Assertions.assertTrue(page.isModified());

            if(page.isFull()) {
                lst.remove(lst.size()-1);
                Assertions.assertThrows(IOException.class, page::save);
            }
            else {
                page.save();
                Assertions.assertFalse(page.isModified());
            }
        }

        storage = BlockStorage.open(tmpFile);
        page = FileBTreePage.open(storage, page.id(), SmallStringSerializer.INSTANCE, IntegerSerializer.INSTANCE);
        Assertions.assertFalse(page.isModified());

        Assertions.assertEquals(lst.size(), page.size());
        for (int i = 0; i < page.size(); i++) {
            Assertions.assertEquals(lst.get(i), page.key(i));
            Assertions.assertEquals(i*2, page.value(i));
        }

        lst.clear();
        for (int i = 0; i < page.size(); i++) {
            lst.add(UUID.randomUUID().toString());

            page.key(i, lst.get(i));
            Assertions.assertEquals(lst.get(i), page.key(i));
            Assertions.assertTrue(page.isModified());

            page.value(i, i*2);
            Assertions.assertEquals(i*2, page.value(i));
            Assertions.assertTrue(page.isModified());

            page.save();
            Assertions.assertFalse(page.isModified());
        }

        Assertions.assertEquals(lst.size(), page.size());
        for (int i = 0; i < page.size(); i++) {
            Assertions.assertEquals(lst.get(i), page.key(i));
            Assertions.assertEquals(i*2, page.value(i));
        }
    }
}

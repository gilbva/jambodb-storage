package com.github.jambodb.storage.pager;

import com.github.jambodb.storage.blocks.BlockStorage;
import com.github.jambodb.storage.types.IntegerSerializer;
import com.github.jambodb.storage.types.SmallStringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class FileBTreePageTest {

    @Test
    public void testBTree() throws IOException {
        var tmpFile = Files.createTempFile("test", "jambodb");
        var storage = BlockStorage.create(tmpFile);
        var page = FileBTreePage.create(storage, true, SmallStringSerializer.INSTANCE, IntegerSerializer.INSTANCE);
        List<String> lst = new ArrayList<>();
        for (int i = 0; !page.isFull(); i++) {
            lst.add(UUID.randomUUID().toString());
            page.size(i+1);
            page.key(i, lst.get(i));
            page.value(i, i+1);
            if(!page.isFull()) {
                page.save();
            }
        }

        storage = BlockStorage.open(tmpFile);
        page = FileBTreePage.load(storage, page.id(), SmallStringSerializer.INSTANCE, IntegerSerializer.INSTANCE);

        Assertions.assertEquals(lst.size(), page.size());
        for (int i = 0; i < page.size(); i++) {
            Assertions.assertEquals(lst.get(i), page.key(i));
            Assertions.assertEquals(i+1, page.value(i));
        }

        lst.clear();
        page.size(page.size()-1);
        page.save();
        for (int i = 0; i < page.size(); i++) {
            lst.add(UUID.randomUUID().toString());
            page.key(i, lst.get(i));
            page.value(i, i+1);
            page.save();
        }

        Assertions.assertEquals(lst.size(), page.size());
        for (int i = 0; i < page.size(); i++) {
            Assertions.assertEquals(lst.get(i), page.key(i));
            Assertions.assertEquals(i+1, page.value(i));
        }
    }
}

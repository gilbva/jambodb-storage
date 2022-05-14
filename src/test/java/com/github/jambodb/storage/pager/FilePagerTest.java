package com.github.jambodb.storage.pager;

import com.github.jambodb.storage.types.IntegerSerializer;
import com.github.jambodb.storage.types.SmallStringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;

public class FilePagerTest {
    @Test
    public void testPager() throws IOException {
        var tmpFile = Files.createTempFile("test", "jambodb");
        var pager = FilePager.create(tmpFile, SmallStringSerializer.INSTANCE, IntegerSerializer.INSTANCE);

        Assertions.assertEquals(0, pager.root());
        Assertions.assertNotNull(pager.page(pager.root()));
        Assertions.assertEquals(0, pager.page(0).id());

        for (int i = 1; i < 100; i++) {
            Assertions.assertNotNull(pager.create(i % 2 == 0));
            Assertions.assertEquals(i, pager.page(i).id());
        }
        pager.fsync();

        Assertions.assertEquals(0, pager.page(0).id());
        for (int i = 1; i < 100; i++) {
            Assertions.assertEquals(i, pager.page(i).id());
        }
        pager.fsync();
    }
}

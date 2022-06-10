package me.gilbva.jambodb.storage.pager;

import me.gilbva.jambodb.storage.types.IntegerSerializer;
import me.gilbva.jambodb.storage.types.SmallStringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;

public class FilePagerTest {
    @Test
    public void testPager() throws IOException {
        var tmpFile = Files.createTempFile("test", "jambodb");
        var pager = FilePager.create(tmpFile, 10, SmallStringSerializer.INSTANCE, IntegerSerializer.INSTANCE);

        Assertions.assertEquals(1, pager.root());
        Assertions.assertNotNull(pager.page(pager.root()));
        Assertions.assertEquals(1, pager.page(1).id());

        for (int i = 2; i < 100; i++) {
            Assertions.assertNotNull(pager.create(i % 2 == 0));
            Assertions.assertEquals(i, pager.page(i).id());
        }
        pager.fsync();

        Assertions.assertEquals(1, pager.page(1).id());
        for (int i = 2; i < 100; i++) {
            Assertions.assertEquals(i, pager.page(i).id());
        }
        pager.fsync();
    }
}

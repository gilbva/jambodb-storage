package com.github.jambodb.storage.blocks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FileBlockStorageTest {
    private static final Random random = new Random();

    @Test
    public void testFileBlockStorage() throws IOException {
        var raf = createFile();
        var storage = BlockStorage.create(raf);

        Assertions.assertThrows(Exception.class, () -> storage.read(0, ByteBuffer.allocate(storage.BLOCK_SIZE)));
        Assertions.assertThrows(Exception.class, () -> storage.write(0, ByteBuffer.allocate(storage.BLOCK_SIZE)));
        for (int i = 0; i < 1000; i++) {
            var toWrite = randomBlock();
            var toRead = ByteBuffer.allocate(storage.BLOCK_SIZE);
            storage.increase();
            storage.write(i, toWrite);
            storage.read(i, toRead);
            Assertions.assertEquals(toWrite, toRead);
        }

        try(var readStorage = BlockStorage.open(raf)) {
            Assertions.assertEquals(storage.count(), readStorage.count());
            for (int i = 0; i < 1000; i++) {
                var toRead = ByteBuffer.allocate(readStorage.BLOCK_SIZE);
                storage.increase();
                storage.read(i, toRead);
            }
        }
    }

    @Test
    public void testFileBlockStorageWithIntValues() throws IOException {
        var raf = createFile();
        try(var storage = BlockStorage.create(raf)) {

            for (int i = 0; i < 10; i++) {
                storage.increase();
                ByteBuffer buffer = ByteBuffer.allocate(storage.BLOCK_SIZE);
                buffer.putInt(i);
                buffer.flip();
                storage.write(i, buffer);
                storage.read(i, buffer);
                Assertions.assertEquals(i, buffer.getInt());
            }

            try (var readStorage = BlockStorage.open(raf)) {
                Assertions.assertEquals(storage.count(), readStorage.count());
                for (int i = 0; i < 10; i++) {
                    var toRead = ByteBuffer.allocate(readStorage.BLOCK_SIZE);
                    readStorage.read(i, toRead);
                    Assertions.assertEquals(i, toRead.getInt());
                }
            }
        }
    }

    private ByteBuffer randomBlock() {
        byte[] arr = new byte[BlockStorage.BLOCK_SIZE];
        random.nextBytes(arr);
        return ByteBuffer.wrap(arr);
    }

    private Path createFile() throws IOException {
        return Files.createTempFile("test-", ".blocks");
    }
}

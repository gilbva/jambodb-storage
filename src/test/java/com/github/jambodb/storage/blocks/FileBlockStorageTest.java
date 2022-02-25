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
        var storage = JamboBlksV1.readWrite(4 * 4096, raf);

        Assertions.assertThrows(Exception.class, () -> storage.read(0, ByteBuffer.allocate(storage.blockSize())));
        Assertions.assertThrows(Exception.class, () -> storage.write(0, ByteBuffer.allocate(storage.blockSize())));
        for (int i = 0; i < 1000; i++) {
            var toWrite = randomBlock(storage.blockSize());
            var toRead = ByteBuffer.allocate(storage.blockSize());
            storage.createBlock();
            storage.write(i, toWrite);
            storage.read(i, toRead);
            Assertions.assertEquals(toWrite, toRead);
        }

        var readStorage = JamboBlksV1.readable(raf);
        Assertions.assertEquals(storage.blockCount(), readStorage.blockCount());
        Assertions.assertEquals(storage.blockSize(), readStorage.blockSize());
        for (int i = 0; i < 1000; i++) {
            var toRead = ByteBuffer.allocate(readStorage.blockSize());
            storage.createBlock();
            storage.read(i, toRead);
        }
    }

    @Test
    public void testFileBlockStorageWithIntValues() throws IOException {
        var raf = createFile();
        var storage = JamboBlksV1.readWrite(4, raf);

        for (int i = 0; i < 10; i++) {
            storage.createBlock();
            ByteBuffer buffer = ByteBuffer.allocate(storage.blockSize());
            buffer.putInt(i);
            buffer.flip();
            storage.write(i, buffer);
            storage.read(i, buffer);
            Assertions.assertEquals(i, buffer.getInt());
        }

        var readStorage = JamboBlksV1.readable(raf);
        Assertions.assertEquals(storage.blockCount(), readStorage.blockCount());
        Assertions.assertEquals(storage.blockSize(), readStorage.blockSize());
        for (int i = 0; i < 10; i++) {
            var toRead = ByteBuffer.allocate(readStorage.blockSize());
            readStorage.read(i, toRead);
            Assertions.assertEquals(i, toRead.getInt());
        }
    }

    private ByteBuffer randomBlock(int blockSize) {
        byte[] arr = new byte[blockSize];
        random.nextBytes(arr);
        return ByteBuffer.wrap(arr);
    }

    private Path createFile() throws IOException {
        return Files.createTempFile("test-", ".blocks");
    }
}

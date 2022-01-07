package com.github.jambodb.storage.blocks;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Random;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FileBlockStorageTest {
    private static final Random random = new Random();

    @Test
    public void testFileBlockStorage() throws IOException {
        var raf = createFile();
        var storage = new FileBlockStorage(4 * 4096, raf);

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

        var readStorage = new FileBlockStorage(raf);
        Assertions.assertEquals(storage.blockCount(), readStorage.blockCount());
        Assertions.assertEquals(storage.blockSize(), readStorage.blockSize());
        for (int i = 0; i < 1000; i++) {
            var toRead = ByteBuffer.allocate(readStorage.blockSize());
            storage.createBlock();
            storage.read(i, toRead);
        }
    }

    private ByteBuffer randomBlock(int blockSize) {
        byte[] arr = new byte[blockSize];
        random.nextBytes(arr);
        return ByteBuffer.wrap(arr);
    }

    private RandomAccessFile createFile() throws IOException {
        File f = File.createTempFile("test-", ".blocks");
        if (f.exists()) {
            f.delete();
        }
        return new RandomAccessFile(f.getAbsolutePath(), "rw");
    }
}

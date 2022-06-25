package me.gilbva.jambodb.storage.blocks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FileBlockStorageTest {
    private static final Random random = new Random();

    @Test
    public void testFileBlockStorage() throws IOException {
        SecurityOptions options = new SecurityOptions(UUID.randomUUID().toString(), "asd");
        testFileBlockStorage(options);
        testFileBlockStorage(null);
    }

    public void testFileBlockStorage(SecurityOptions opts) throws IOException {
        var raf = createFile();
        try(var storage = BlockStorage.create(raf, opts)) {
            Assertions.assertThrows(Exception.class, () -> storage.read(1, ByteBuffer.allocate(storage.BLOCK_SIZE)));
            Assertions.assertThrows(Exception.class, () -> storage.write(1, ByteBuffer.allocate(storage.BLOCK_SIZE)));
            List<ByteBuffer> buffers = new ArrayList<>();
            for (int i = 1; i <= 100; i++) {
                var toWrite = randomBlock();
                buffers.add(toWrite);
                var toRead = ByteBuffer.allocate(storage.BLOCK_SIZE);
                storage.increase();
                storage.write(i, toWrite);
                storage.read(i, toRead);
                Assertions.assertArrayEquals(toWrite.array(), toRead.array());
            }

            try (var readStorage = BlockStorage.open(raf, opts)) {
                Assertions.assertEquals(storage.count(), readStorage.count());
                for (int i = 1; i <= 100; i++) {
                    var toRead = ByteBuffer.allocate(readStorage.BLOCK_SIZE);
                    readStorage.read(i, toRead);
                    Assertions.assertArrayEquals(buffers.get(i-1).array(), toRead.array());
                }
            }
        }
    }

    @Test
    public void testFileBlockStorageWithIntValues() throws IOException {
        SecurityOptions options = new SecurityOptions(UUID.randomUUID().toString(), "asd");
        testFileBlockStorageWithIntValues(null);
        testFileBlockStorageWithIntValues(options);
    }

    public void testFileBlockStorageWithIntValues(SecurityOptions opts) throws IOException {
        var raf = createFile();
        int count = 0;
        try(var storage = BlockStorage.create(raf, opts)) {

            for (int i = 1; i <= 10; i++) {
                storage.increase();
                ByteBuffer buffer = ByteBuffer.allocate(storage.BLOCK_SIZE);
                buffer.putInt(i);
                buffer.flip();
                storage.write(i, buffer);
                storage.read(i, buffer);
                Assertions.assertEquals(i, buffer.getInt());
            }

            count = storage.count();
        }

        try (var readStorage = BlockStorage.open(raf, opts)) {
            Assertions.assertEquals(count, readStorage.count());
            for (int i = 1; i <= 10; i++) {
                var toRead = ByteBuffer.allocate(readStorage.BLOCK_SIZE);
                readStorage.read(i, toRead);
                Assertions.assertEquals(i, toRead.getInt());
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

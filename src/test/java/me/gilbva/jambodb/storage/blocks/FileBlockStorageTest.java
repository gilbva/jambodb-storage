package me.gilbva.jambodb.storage.blocks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.crypto.NoSuchPaddingException;

public class FileBlockStorageTest {
    private static final Random random = new Random();

    @Test
    public void testFileBlockStorage() throws IOException {
        testFileBlockStorage(UUID.randomUUID().toString());
        testFileBlockStorage(null);
    }

    public void testFileBlockStorage(String password) throws IOException {
        var raf = createFile();
        try(var storage = BlockStorage.create(raf, password)) {
            Assertions.assertThrows(Exception.class, () -> storage.read(0, ByteBuffer.allocate(storage.BLOCK_SIZE)));
            Assertions.assertThrows(Exception.class, () -> storage.write(0, ByteBuffer.allocate(storage.BLOCK_SIZE)));
            List<ByteBuffer> buffers = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                var toWrite = randomBlock();
                buffers.add(toWrite);
                var toRead = ByteBuffer.allocate(storage.BLOCK_SIZE);
                storage.increase();
                storage.write(i, toWrite);
                storage.read(i, toRead);
                Assertions.assertArrayEquals(toWrite.array(), toRead.array());
            }

            try (var readStorage = BlockStorage.open(raf, password)) {
                Assertions.assertEquals(storage.count(), readStorage.count());
                for (int i = 0; i < 100; i++) {
                    var toRead = ByteBuffer.allocate(readStorage.BLOCK_SIZE);
                    readStorage.read(i, toRead);
                    Assertions.assertArrayEquals(buffers.get(i).array(), toRead.array());
                }
            }
        }
    }

    @Test
    public void testFileBlockStorageWithIntValues() throws IOException {
        testFileBlockStorageWithIntValues(null);
        testFileBlockStorageWithIntValues("asd");
    }

    public void testFileBlockStorageWithIntValues(String password) throws IOException {
        var raf = createFile();
        try(var storage = BlockStorage.create(raf, password)) {

            for (int i = 0; i < 10; i++) {
                storage.increase();
                ByteBuffer buffer = ByteBuffer.allocate(storage.BLOCK_SIZE);
                buffer.putInt(i);
                buffer.flip();
                storage.write(i, buffer);
                storage.read(i, buffer);
                Assertions.assertEquals(i, buffer.getInt());
            }

            try (var readStorage = BlockStorage.open(raf, password)) {
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

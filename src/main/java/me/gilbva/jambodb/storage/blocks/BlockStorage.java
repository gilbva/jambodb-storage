package me.gilbva.jambodb.storage.blocks;

import javax.crypto.NoSuchPaddingException;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

/**
 * This class represents a block storage, block is defined as a collections of bytes that are
 * all off the same size, the purpose of this class is to handle the underlying storage and give access
 * to it by reading and writing arrays of bytes that can be identified by their index.
 * This class is intended to by implemented to access external memory like a single file but the
 * actual implementation could be anything from main memory access to network access, this interface
 * makes no assumptions about it.
 *
 */
public interface BlockStorage extends Closeable {

    int BLOCK_SIZE = 4095;

    static BlockStorage open(Path path) throws IOException {
        OpenOption[] options = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE};
        SeekableByteChannel channel = Files.newByteChannel(path, options);
        return new JamboBlksV1(channel, false, null);
    }

    static BlockStorage create(Path path) throws IOException {
        OpenOption[] options;
        if (Files.exists(path)) {
            options = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE};
        }
        else {
            options = new OpenOption[]{StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE};
        }
        SeekableByteChannel channel = Files.newByteChannel(path, options);
        return new JamboBlksV1(channel, true, null);
    }

    static BlockStorage open(Path path, String password) throws IOException {
        if(password == null || password.isEmpty()) {
            return open(path);
        }
        OpenOption[] options = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE};
        SeekableByteChannel channel = Files.newByteChannel(path, options);
        try {
            return new JamboBlksV1(channel, false, new BlockCipher(password));
        }
        catch (NoSuchAlgorithmException | InvalidKeySpecException | InvalidAlgorithmParameterException |
               InvalidKeyException | NoSuchPaddingException e) {
            throw new IOException(e);
        }
    }

    static BlockStorage create(Path path, String password) throws IOException {
        if(password == null || password.isEmpty()) {
            return create(path);
        }
        OpenOption[] options;
        if (Files.exists(path)) {
            options = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE};
        }
        else {
            options = new OpenOption[]{StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE};
        }
        SeekableByteChannel channel = Files.newByteChannel(path, options);
        try {
            return new JamboBlksV1(channel, true, new BlockCipher(password));
        }
        catch (NoSuchAlgorithmException | InvalidKeySpecException | InvalidAlgorithmParameterException |
                 InvalidKeyException | NoSuchPaddingException e) {
            throw new IOException(e);
        }
    }

    /**
     * Gets the amount of blocks that have been created in this storage.
     *
     * @return An integer that represents the count of blocks present in this storage.
     */
    int count();

    /**
     * This expands the count of blocks currently manage by this object.
     *
     * @return The index of the new block created.
     * @throws IOException if any I/O exceptions occur writing to the underlying storage.
     */
    int increase() throws IOException;

    /**
     * Reads the data space of the header of the storage.
     *
     * @param data the buffer to read into.
     * @throws IOException if any I/O exception occurs.
     */
    void readHead(ByteBuffer data) throws IOException;

    /**
     * Writes the data space of the header of the storage.
     *
     * @param data the buffer to write to.
     * @throws IOException if any I/O exception occurs.
     */
    void writeHead(ByteBuffer data) throws IOException;

    /**
     * This method reads the block at the given index, into the provided data buffer.
     *
     * @param index The index of the block to read from.
     * @param data The buffer to place the data.
     * @throws IOException if any I/O exceptions occur reading to the underlying storage.
     */
    void read(int index, ByteBuffer data) throws IOException;

    /**
     * This method reads the block at the given index, into the provided data buffer.
     *
     * @param index The index of the block to write to.
     * @param data The buffer with the data to be written to the block.
     * @throws IOException if any I/O exceptions occur writing to the underlying storage.
     */
    void write(int index, ByteBuffer data) throws IOException;
}

package me.gilbva.jambodb.storage.blocks;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

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

    int HEAD_SIZE = BLOCK_SIZE - 20;

    /**
     * Opens the given file, as a block storage, if the file does not have a recognizable
     * format, an I/O exception is thrown.
     *
     * @param path the file to open.
     * @param opts the security options for the file, must be the same as the one used
     *             when the file was created.
     * @return An instance of this interface that can be used to access the blocks in the given file.
     * @throws IOException If the file type is incorrect, or the security options provided are invalid.
     */
    static BlockStorage open(Path path, SecurityOptions opts) throws IOException {
        var handler = new JamboBlksV1();
        handler.open(path, opts);
        return handler;
    }

    /**
     * Creates a new block storage in the file at the given path, that can be used to create, read and
     * write blocks of data.
     *
     * @param path the path to create the block storage at.
     * @param opts the security options for the new storage.
     * @return An instance of this interface that can be used to access the blocks in the given file.
     * @throws IOException If any I/O error occurs initializing the file.
     */
    static BlockStorage create(Path path, SecurityOptions opts) throws IOException {
        var handler = new JamboBlksV1();
        handler.create(path, opts);
        return handler;
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
     * @param id The index of the block to read from.
     * @param data The buffer to place the data.
     * @throws IOException if any I/O exceptions occur reading to the underlying storage.
     */
    void read(int id, ByteBuffer data) throws IOException;

    /**
     * This method reads the block at the given index, into the provided data buffer.
     *
     * @param id The index of the block to write to.
     * @param data The buffer with the data to be written to the block.
     * @throws IOException if any I/O exceptions occur writing to the underlying storage.
     */
    void write(int id, ByteBuffer data) throws IOException;
}

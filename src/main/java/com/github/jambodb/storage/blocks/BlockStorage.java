package com.github.jambodb.storage.blocks;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class represents a block storage, block is defined as a collections of bytes that are
 * all off the same size, the purpose of this class is to handle the underlying storage and give access
 * to it by reading and writing arrays of bytes that can be identified by their index.
 * This class is intended to by implemented to access external memory like a single file but the
 * actual implementation could be anything from main memory access to network access, this interface
 * makes no assumptions about it.
 */
public interface BlockStorage {
    /**
     * Gets the size in bytes of the blocks handled by this storage.
     * (All blocks in this storage are of the same size.)
     *
     * @return An integer that represents the size in bytes of the blocks handled by this storage.
     */
    int blockSize();

    /**
     * Gets the amount of blocks that have been created in this storage.
     *
     * @return An integer that represents the count of blocks present in this storage.
     */
    int blockCount();

    /**
     * @return
     * @throws IOException
     */
    int createBlock() throws IOException;

    /**
     * @param index
     * @param data
     * @throws IOException
     */
    void read(int index, ByteBuffer data) throws IOException;

    /**
     * @param index
     * @param data
     * @throws IOException
     */
    void write(int index, ByteBuffer data) throws IOException;
}

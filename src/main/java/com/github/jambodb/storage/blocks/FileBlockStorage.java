package com.github.jambodb.storage.blocks;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileBlockStorage implements BlockStorage {

    private static final int HEADER_SIZE = 8;

    private int blockSize;

    private int blockCount;

    private final RandomAccessFile raf;

    private final FileChannel channel;

    public FileBlockStorage(int blockSize, RandomAccessFile raf) throws IOException {
        this.blockSize = blockSize;
        this.blockCount = 0;
        this.raf = raf;
        this.raf.setLength(0);
        this.channel = raf.getChannel();
    }

    public FileBlockStorage(RandomAccessFile raf) throws IOException {
        this.raf = raf;
        this.channel = raf.getChannel();
        readHeader();
    }

    @Override
    public int blockSize() {
        return blockSize;
    }

    @Override
    public int blockCount() {
        return blockCount;
    }

    @Override
    public synchronized int createBlock() throws IOException {
        blockCount++;
        writeHeader();
        return blockCount;
    }

    @Override
    public synchronized void read(int index, ByteBuffer data) throws IOException {
        if(index >= blockCount) {
            throw new IndexOutOfBoundsException("The block does not exists");
        }
        long pos = findPosition(index);
        channel.position(pos);
        channel.read(data);
        data.flip();
    }

    @Override
    public synchronized void write(int index, ByteBuffer data) throws IOException {
        if(index >= blockCount) {
            throw new IndexOutOfBoundsException("The block does not exists;");
        }
        if(data.remaining() > blockSize) {
            throw new IOException("Block overflow");
        }
        long pos = findPosition(index);
        channel.position(pos);
        channel.write(data);
        data.flip();
    }

    private long findPosition(int index) {
        return HEADER_SIZE + ((long)index * blockSize);
    }

    private void writeHeader() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        buffer.putInt(blockSize);
        buffer.putInt(blockCount);
        buffer.flip();
        channel.position(0);
        channel.write(buffer);
    }

    private void readHeader() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        channel.position(0);
        channel.read(buffer);
        buffer.flip();
        blockSize = buffer.getInt();
        blockCount = buffer.getInt();
    }
}

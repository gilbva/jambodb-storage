package com.github.jambodb.storage.blocks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class JamboBlksV1 implements BlockStorage {

    private final SeekableByteChannel channel;
    private final HeaderBlksV1 header;
    private int count;

    JamboBlksV1(SeekableByteChannel fileChannel, boolean init) throws IOException {
        channel = fileChannel;

        header = new HeaderBlksV1();
        if(init) {
            header.init();
            header.count(0);
            header.write(channel);
        }
        else {
            header.read(channel);
            count = header.count();
        }
    }

    @Override
    public int count() {
        return count;
    }

    @Override
    public void count(int count) throws IOException {
        this.count = count;
        header.count(count);
        header.write(channel);
    }

    @Override
    public synchronized int increase() throws IOException {
        count(count+1);
        return count-1;
    }

    @Override
    public void readHead(ByteBuffer data) throws IOException {
        header.read(data);
    }

    @Override
    public void writeHead(ByteBuffer data) throws IOException {
        header.write(data);
    }

    @Override
    public synchronized void read(int index, ByteBuffer data) throws IOException {
        if (index >= count) {
            throw new IndexOutOfBoundsException("the block does not exists");
        }
        long pos = findPosition(index);
        channel.position(pos);
        channel.read(data);
        data.flip();
    }

    @Override
    public synchronized void write(int index, ByteBuffer data) throws IOException {
        if (index >= count) {
            throw new IndexOutOfBoundsException("the block does not exists");
        }
        if (data.remaining() > BLOCK_SIZE) {
            throw new IOException("block overflow");
        }
        long pos = findPosition(index);
        channel.position(pos);
        channel.write(data);
        data.flip();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private long findPosition(int index) {
        return BLOCK_SIZE + ((long) index * BLOCK_SIZE);
    }
}

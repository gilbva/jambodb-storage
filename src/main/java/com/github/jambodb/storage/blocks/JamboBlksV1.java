package com.github.jambodb.storage.blocks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class JamboBlksV1 implements BlockStorage {
    private final SeekableByteChannel channel;
    private JamboBlksV1Header header;
    private int blockSize;
    private int blockCount;

    public static BlockStorage writeable(int blockSize, Path path) throws IOException {
        OpenOption[] options;
        if (Files.exists(path)) {
            options = new OpenOption[]{StandardOpenOption.WRITE};
        } else {
            options = new OpenOption[]{StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE};
        }
        SeekableByteChannel channel = Files.newByteChannel(path, options);
        return new JamboBlksV1(blockSize, channel);
    }

    public static BlockStorage readable(Path path) throws IOException {
        OpenOption[] options = new OpenOption[]{StandardOpenOption.READ};
        SeekableByteChannel channel = Files.newByteChannel(path, options);
        return new JamboBlksV1(channel);
    }

    public static BlockStorage readWrite(int blockSize, Path path) throws IOException {
        OpenOption[] options;
        if (Files.exists(path)) {
            options = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE};
        } else {
            options = new OpenOption[]{StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE};
        }
        SeekableByteChannel channel = Files.newByteChannel(path, options);
        return new JamboBlksV1(blockSize, channel);
    }

    private JamboBlksV1(int size, SeekableByteChannel fileChannel) throws IOException {
        channel = fileChannel;
        blockSize = size;

        header = new JamboBlksV1Header();
        header.init();
        header.blockSize(blockSize);
        header.blockCount(0);
        header.write(channel);
    }

    private JamboBlksV1(SeekableByteChannel fileChannel) throws IOException {
        channel = fileChannel;
        header = new JamboBlksV1Header();
        header.read(channel);

        blockCount = header.blockCount();
        blockSize = header.blockSize();
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
    public void blockCount(int count) throws IOException {
        blockCount = count;
        header.blockCount(blockCount);
        header.write(channel);
    }

    @Override
    public synchronized int createBlock() throws IOException {
        blockCount(blockCount+1);
        return blockCount;
    }

    @Override
    public synchronized void read(int index, ByteBuffer data) throws IOException {
        if (index >= blockCount) {
            throw new IndexOutOfBoundsException("The block does not exists");
        }
        long pos = findPosition(index);
        channel.position(pos);
        channel.read(data);
        data.flip();
    }

    @Override
    public synchronized void write(int index, ByteBuffer data) throws IOException {
        if (index >= blockCount) {
            throw new IndexOutOfBoundsException("The block does not exists");
        }
        if (data.remaining() > blockSize) {
            throw new IOException("Block overflow");
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
        return JamboBlksV1Header.HEADER_SIZE + ((long) index * blockSize);
    }
}

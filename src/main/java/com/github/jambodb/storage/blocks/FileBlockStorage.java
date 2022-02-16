package com.github.jambodb.storage.blocks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public class FileBlockStorage implements BlockStorage {
    private static final byte[] SIGN = "JamboBlks".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VERSION = "v1".getBytes(StandardCharsets.UTF_8);
    private static final int HEADER_SIZE = SIGN.length + VERSION.length + 8;

    private final SeekableByteChannel channel;
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
        return new FileBlockStorage(blockSize, channel);
    }

    public static BlockStorage readable(Path path) throws IOException {
        OpenOption[] options = new OpenOption[]{StandardOpenOption.READ};
        SeekableByteChannel channel = Files.newByteChannel(path, options);
        return new FileBlockStorage(channel);
    }

    public static BlockStorage readWrite(int blockSize, Path path) throws IOException {
        OpenOption[] options;
        if (Files.exists(path)) {
            options = new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE};
        } else {
            options = new OpenOption[]{StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE};
        }
        SeekableByteChannel channel = Files.newByteChannel(path, options);
        return new FileBlockStorage(blockSize, channel);
    }

    public FileBlockStorage(int blockSize, SeekableByteChannel channel) {
        this.blockSize = blockSize;
        this.blockCount = 0;
        this.channel = channel;
    }

    public FileBlockStorage(SeekableByteChannel channel) throws IOException {
        this.channel = channel;
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
    public void blockCount(int count) throws IOException {
        blockCount = count;
        writeHeader();
    }

    @Override
    public synchronized int createBlock() throws IOException {
        blockCount++;
        writeHeader();
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
            throw new IndexOutOfBoundsException("The block does not exists;");
        }
        if (data.remaining() > blockSize) {
            throw new IOException("Block overflow");
        }
        long pos = findPosition(index);
        channel.position(pos);
        channel.write(data);
        data.flip();
    }

    private long findPosition(int index) {
        return HEADER_SIZE + ((long) index * blockSize);
    }

    private void writeHeader() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        buffer.put(SIGN);
        buffer.put(VERSION);
        buffer.putInt(blockSize);
        buffer.putInt(blockCount);
        buffer.flip();
        channel.position(0);
        channel.write(buffer);
    }

    private void readHeader() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        channel.position(0);
        int readed = channel.read(buffer);
        if(readed != HEADER_SIZE) {
            throw new IOException("invalid file");
        }
        buffer.flip();
        validate(buffer);
        blockSize = buffer.getInt();
        blockCount = buffer.getInt();
    }

    private void validate(ByteBuffer buffer) throws IOException {
        byte[] fileSign = new byte[SIGN.length];
        byte[] fileVersion = new byte[VERSION.length];

        buffer.get(fileSign);
        buffer.get(fileVersion);

        if(!Arrays.equals(fileSign, SIGN) || !Arrays.equals(fileVersion, VERSION)) {
            throw new IOException("invalid file");
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}

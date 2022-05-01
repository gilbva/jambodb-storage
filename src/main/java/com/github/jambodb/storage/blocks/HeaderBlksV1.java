package com.github.jambodb.storage.blocks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import static com.github.jambodb.storage.blocks.BlockStorage.BLOCK_SIZE;

class HeaderBlksV1 {
    private static final byte[] TITLE = "JamboBlks".getBytes(StandardCharsets.UTF_8);

    private static final byte[] VERSION = "v1".getBytes(StandardCharsets.UTF_8);

    private static final int BLOCK_COUNT_POS = TITLE.length + VERSION.length + 4;

    private static final int DATA_POS = BLOCK_COUNT_POS + 4;

    private final MessageDigest digest;

    private final ByteBuffer content;

    public HeaderBlksV1() throws IOException {
        content = ByteBuffer.allocate(BLOCK_SIZE);
        try {
            this.digest = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException ex) {
            throw new IOException(ex.getMessage(), ex);
        }
    }

    public String title() {
        return verifyAndReturn(0, TITLE);
    }

    public String version() {
        return verifyAndReturn(TITLE.length, VERSION);
    }

    public void init() {
        content.position(0);
        content.put(TITLE);
        content.put(VERSION);
    }

    public int count() {
        return content.getInt(BLOCK_COUNT_POS);
    }

    public void count(int value) {
        content.putInt(BLOCK_COUNT_POS, value);
    }

    public void read(ByteBuffer data) {
        int size = BLOCK_SIZE - DATA_POS - digest.getDigestLength();
        data.put(content.array(), DATA_POS, Math.min(size, data.remaining()));
        data.flip();
    }

    public void write(ByteBuffer data) throws IOException {
        int size = BLOCK_SIZE - DATA_POS - digest.getDigestLength();
        if (data.remaining() > size) {
            throw new IOException("header data overflow");
        }
        content.position(DATA_POS);
        content.put(data.array());
    }

    public byte[] calcChecksum() {
        byte[] dataBytes = new byte[BLOCK_SIZE - digest.getDigestLength() - BLOCK_COUNT_POS];
        content.position(BLOCK_COUNT_POS);
        content.get(dataBytes);
        return digest.digest(dataBytes);
    }

    public byte[] checksum() {
        int checksumPos = BLOCK_SIZE - digest.getDigestLength();
        byte[] data = new byte[digest.getDigestLength()];
        content.position(checksumPos);
        content.get(data);
        return data;
    }

    public void checksum(byte[] value) {
        if(value.length != digest.getDigestLength()) {
            throw new IllegalArgumentException("invalid checksum");
        }
        int checksumPos = BLOCK_SIZE - digest.getDigestLength();
        content.position(checksumPos);
        content.put(value);
    }

    public void write(SeekableByteChannel channel) throws IOException {
        checksum(calcChecksum());
        channel.position(0);
        content.position(0);
        if(content.remaining() != BLOCK_SIZE) {
            throw new IOException("header overflow");
        }
        if(channel.write(content) != BLOCK_SIZE) {
            throw new IOException("invalid header size");
        }
    }

    public void read(SeekableByteChannel channel) throws IOException {
        channel.position(0);
        content.position(0);
        if(content.remaining() != BLOCK_SIZE) {
            throw new IOException("header overflow");
        }
        if(channel.read(content) != BLOCK_SIZE) {
            throw new IOException("invalid header size");
        }

        byte[] calc = calcChecksum();
        byte[] checksum = checksum();

        if(title() == null) {
            throw new IOException("invalid file format.");
        }

        if(version() == null) {
            throw new IOException("invalid file version.");
        }

        if(!Arrays.equals(calc, checksum)) {
            throw new IOException("file header has been corrupted.");
        }
    }

    private String verifyAndReturn(int position, byte[] tmpl) {
        byte[] data = new byte[tmpl.length];
        content.position(position);
        if(data.length > content.remaining()) {
            return null;
        }

        content.get(data);
        if(Arrays.equals(data, tmpl)) {
            return new String(data, StandardCharsets.UTF_8);
        }
        return null;
    }
}

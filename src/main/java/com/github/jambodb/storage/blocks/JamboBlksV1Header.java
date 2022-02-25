package com.github.jambodb.storage.blocks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class JamboBlksV1Header {
    private static final byte[] TITLE = "JamboBlks".getBytes(StandardCharsets.UTF_8);

    private static final byte[] VERSION = "v1".getBytes(StandardCharsets.UTF_8);

    private static final int BLOCK_SIZE_POS = TITLE.length + VERSION.length;

    private static final int BLOCK_COUNT_POS = BLOCK_SIZE_POS + 4;

    public static final int HEADER_SIZE = 512;

    private static final int CHECKSUM_SIZE_POS = HEADER_SIZE - 4;

    private final MessageDigest digest;

    private ByteBuffer content;

    public JamboBlksV1Header() throws IOException {
        content = ByteBuffer.allocate(JamboBlksV1Header.HEADER_SIZE);
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

    public int blockSize() {
        return content.getInt(BLOCK_SIZE_POS);
    }

    public void blockSize(int value) {
        content.putInt(BLOCK_SIZE_POS, value);
    }

    public int blockCount() {
        return content.getInt(BLOCK_COUNT_POS);
    }

    public void blockCount(int value) {
        content.putInt(BLOCK_COUNT_POS, value);
    }

    public byte[] calcChecksum() {
        byte[] dataBytes = new byte[8];
        content.position(BLOCK_SIZE_POS);
        content.get(dataBytes);
        return digest.digest(dataBytes);
    }

    public byte[] checksum() {
        int checksumSize = content.getInt(CHECKSUM_SIZE_POS);
        byte[] data = new byte[checksumSize];
        content.position(CHECKSUM_SIZE_POS - checksumSize);
        content.get(data);
        return data;
    }

    public void checksum(byte[] value) {
        content.position(CHECKSUM_SIZE_POS - value.length);
        content.put(value);
        content.putInt(CHECKSUM_SIZE_POS, value.length);
    }

    public void write(SeekableByteChannel channel) throws IOException {
        checksum(calcChecksum());
        channel.position(0);
        content.position(0);
        if(content.remaining() != HEADER_SIZE) {
            throw new IOException("header overflow");
        }
        if(channel.write(content) != HEADER_SIZE) {
            throw new IOException("Invalid header size");
        }
    }

    public void read(SeekableByteChannel channel) throws IOException {
        channel.position(0);
        content.position(0);
        if(content.remaining() != HEADER_SIZE) {
            throw new IOException("header overflow");
        }
        if(channel.read(content) != HEADER_SIZE) {
            throw new IOException("Invalid header size");
        }

        byte[] calc = calcChecksum();
        byte[] checksum = checksum();

        if(title() == null) {
            throw new IOException("Invalid file format.");
        }

        if(version() == null) {
            throw new IOException("Invalid file version.");
        }

        if(!Arrays.equals(calc, checksum)) {
            throw new IOException("File header has been corrupted.");
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

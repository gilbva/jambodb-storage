package me.gilbva.jambodb.storage.blocks;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Header wrapper for JamboBlksV1
 */
class HeaderBlksV1 {
    private static final byte[] TITLE = "JamboBlks".getBytes(StandardCharsets.UTF_8);

    private static final byte[] VERSION = "v1".getBytes(StandardCharsets.UTF_8);

    private static final int BLOCK_COUNT_POS = TITLE.length + VERSION.length + 4;

    private static final int DATA_POS = BLOCK_COUNT_POS + 4;

    private final MessageDigest digest;

    private final ByteBuffer content;

    /**
     * Creates an empty header that can be fill with the actual data from a file.
     *
     * @throws IOException if any I/O exception occurs.
     */
    public HeaderBlksV1() throws IOException {
        content = ByteBuffer.allocate(BlockStorage.BLOCK_SIZE);
        try {
            this.digest = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException ex) {
            throw new IOException(ex.getMessage(), ex);
        }
    }

    /**
     * Gets and verify the title of the file.
     *
     * @return the title must be 'JamboBlks'
     */
    public String title() {
        return verifyAndReturn(0, TITLE);
    }

    /**
     * Gets and verify the version of the file.
     *
     * @return the version must be 'v1'
     */
    public String version() {
        return verifyAndReturn(TITLE.length, VERSION);
    }

    /**
     * Initializes this header by adding title and version.
     */
    public void init() {
        content.position(0);
        content.put(TITLE);
        content.put(VERSION);
    }

    /**
     * Gets the count of blocks saved in the header.
     *
     * @return the number of blocks saved.
     */
    public int count() {
        return content.getInt(BLOCK_COUNT_POS);
    }

    /**
     * Writes the number of blocks for the storage in this header.
     *
     * @param value the number of blocks.
     */
    public void count(int value) {
        content.putInt(BLOCK_COUNT_POS, value);
    }

    /**
     * Reads the header from the given buffer.
     *
     * @param data the buffer containen the data for the header.
     */
    public void read(ByteBuffer data) {
        int size = BlockStorage.BLOCK_SIZE - DATA_POS - digest.getDigestLength();
        data.put(content.array(), DATA_POS, Math.min(size, data.remaining()));
        data.flip();
    }

    /**
     * Writes the header to the given buffer.
     *
     * @param data the buffer to write to.
     * @throws IOException if the buffer provided does not have enough space
     */
    public void write(ByteBuffer data) throws IOException {
        int size = BlockStorage.BLOCK_SIZE - DATA_POS - digest.getDigestLength();
        if (data.remaining() > size) {
            throw new BufferOverflowException();
        }
        content.position(DATA_POS);
        content.put(data.array());
    }

    /**
     * Calculates the checksum of the header.
     *
     * @return the checksum for all the bytes in the header.
     */
    public byte[] calcChecksum() {
        byte[] dataBytes = new byte[BlockStorage.BLOCK_SIZE - digest.getDigestLength() - BLOCK_COUNT_POS];
        content.position(BLOCK_COUNT_POS);
        content.get(dataBytes);
        return digest.digest(dataBytes);
    }

    /**
     * Gets the checksum stored in the header
     *
     * @return the previously calculated checksum for this header
     */
    public byte[] checksum() {
        int checksumPos = BlockStorage.BLOCK_SIZE - digest.getDigestLength();
        byte[] data = new byte[digest.getDigestLength()];
        content.position(checksumPos);
        content.get(data);
        return data;
    }

    /**
     * Stores a checksum of the data of the header
     *
     * @param value the calculated checksum to store
     */
    public void checksum(byte[] value) {
        if(value.length != digest.getDigestLength()) {
            throw new IllegalArgumentException("invalid checksum");
        }
        int checksumPos = BlockStorage.BLOCK_SIZE - digest.getDigestLength();
        content.position(checksumPos);
        content.put(value);
    }

    /**
     * Writes the content of the header to the given channel
     *
     * @param channel the channel to write the header to.
     * @throws IOException if any I/O exception occurs.
     */
    public void write(SeekableByteChannel channel) throws IOException {
        checksum(calcChecksum());
        channel.position(0);
        content.position(0);
        if(content.remaining() != BlockStorage.BLOCK_SIZE) {
            throw new IOException("header overflow");
        }
        if(channel.write(content) != BlockStorage.BLOCK_SIZE) {
            throw new IOException("invalid header size");
        }
    }

    /**
     * Reads the content of the header from the given channel.
     *
     * @param channel the channel to read the data from.
     * @throws IOException if any I/O exception occurs.
     */
    public void read(SeekableByteChannel channel) throws IOException {
        channel.position(0);
        content.position(0);
        if(content.remaining() != BlockStorage.BLOCK_SIZE) {
            throw new IOException("header overflow");
        }
        if(channel.read(content) != BlockStorage.BLOCK_SIZE) {
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

    /**
     * Verifies if the given bytes correspond to the bytes
     * at the given position in the internal buffer
     *
     * @param position the position to read the bytes from.
     * @param tmpl the bytes to match with those read from the internal buffer
     * @return null if the bytes do not match, or the string if they do
     */
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

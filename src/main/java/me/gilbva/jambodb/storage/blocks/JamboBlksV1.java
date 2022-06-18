package me.gilbva.jambodb.storage.blocks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

/**
 * File format JamboBlks version 1, implements the BlockStorage interface.
 */
class JamboBlksV1 implements BlockStorage {

    static final int INTERNAL_BLOCK_SIZE = BLOCK_SIZE + 1;

    private final SeekableByteChannel channel;
    private final HeaderBlksV1 header;
    private int count;
    private BlockCipher cipher;

    /**
     * This constructor accepts the underlying file channel and if it needs to be initialized
     *
     * @param fileChannel the channel to the file
     * @param init if the file needs to be initialized.
     * @throws IOException if any I/O exception occurs
     */
    JamboBlksV1(SeekableByteChannel fileChannel, boolean init, BlockCipher blockCipher) throws IOException {
        channel = fileChannel;
        cipher = blockCipher;
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
    public void readHead(ByteBuffer data) {
        header.read(data);
    }

    @Override
    public void writeHead(ByteBuffer data) throws IOException {
        header.write(data);
        header.write(channel);
    }

    @Override
    public synchronized void read(int index, ByteBuffer data) throws IOException {
        if (index >= count) {
            throw new IndexOutOfBoundsException("the block does not exists");
        }
        if (data.limit() > BLOCK_SIZE) {
            throw new IOException("block overflow");
        }
        long pos = findPosition(index);
        channel.position(pos);

        if(cipher != null) {
            try {
                ByteBuffer temp = ByteBuffer.allocate(JamboBlksV1.INTERNAL_BLOCK_SIZE);
                channel.read(temp);
                byte[] decBytes = cipher.decrypt(temp.array());

                data.position(0);
                data.limit(decBytes.length);
                data.put(decBytes);
                data.flip();
            }
            catch (Exception e) {
                throw new IOException(e);
            }
        }
        else {
            channel.read(data);
            data.flip();
        }
    }

    @Override
    public synchronized void write(int index, ByteBuffer data) throws IOException {
        if (index >= count) {
            throw new IndexOutOfBoundsException("the block does not exists");
        }
        if (data.limit() > BLOCK_SIZE) {
            throw new IOException("block overflow");
        }
        long pos = findPosition(index);
        channel.position(pos);

        if(cipher != null) {
            try {
                byte[] encBytes = new byte[BLOCK_SIZE];
                data.position(0);
                data.limit(BLOCK_SIZE);
                data.get(encBytes);
                data.flip();

                encBytes = cipher.encrypt(encBytes);
                channel.write(ByteBuffer.wrap(encBytes));
            }
            catch (Exception e) {
                throw new IOException(e);
            }
        }
        else {
            channel.write(data);
            data.flip();
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    /**K
     * Finds the position of the block given its index.
     *
     * @param index the zero-based index of the block.
     * @return the position of the block inside the file.
     */
    private long findPosition(int index) {
        return INTERNAL_BLOCK_SIZE + ((long) index * INTERNAL_BLOCK_SIZE);
    }
}

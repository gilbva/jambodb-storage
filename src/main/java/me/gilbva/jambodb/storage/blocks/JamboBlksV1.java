package me.gilbva.jambodb.storage.blocks;

import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.spec.KeySpec;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

class JamboBlksV1 implements BlockStorage {

    private static final int BLOCK_REAL_SIZE = 4096;

    private static final int BLOCK_DATA_SIZE = BLOCK_REAL_SIZE - 1;

    private static int INIT_DATA_SIZE = 16;

    private static int HEAD_REAL_SIZE = BLOCK_REAL_SIZE - INIT_DATA_SIZE;

    private static int HEAD_DATA_SIZE = HEAD_REAL_SIZE - 1;

    private static int HEAD_USER_DATA_SIZE = HEAD_DATA_SIZE - 4;

    private static final byte[] TITLE = "JamboBlks".getBytes(StandardCharsets.UTF_8);

    private static final short VERSION = 1;

    private SeekableByteChannel channel;

    private boolean encrypted;

    private SecurityOptions options;

    private Cipher encCipher;

    private Cipher decCipher;

    private ByteBuffer headData;

    @Override
    public int count() {
        return headData.getInt(0);
    }

    @Override
    public int increase() throws IOException {
        int count = headData.getInt(0);
        headData.putInt(0, count+1);
        writeHeader();
        return count;
    }

    @Override
    public void readHead(ByteBuffer data) throws IOException {
        if(data.capacity() > HEAD_USER_DATA_SIZE) {
            throw new IllegalArgumentException("invalid data size");
        }

        byte[] userData = new byte[HEAD_USER_DATA_SIZE];
        headData.position(4);
        headData.get(userData);

        data.position(0);
        data.limit(HEAD_USER_DATA_SIZE);
        data.put(userData);
        data.flip();
    }

    @Override
    public void writeHead(ByteBuffer data) throws IOException {
        if(data.capacity() > HEAD_USER_DATA_SIZE) {
            throw new IllegalArgumentException("invalid data size");
        }

        byte[] userData = new byte[HEAD_USER_DATA_SIZE];
        data.position(0);
        data.limit(HEAD_USER_DATA_SIZE);
        data.get(userData);
        data.flip();

        headData.position(4);
        headData.put(userData);
        writeHeader();
    }

    @Override
    public void read(int id, ByteBuffer data) throws IOException {
        if(id <= 0 || id > count()) {
            throw new IllegalArgumentException("invalid block id " + id);
        }
        if(data.capacity() > BLOCK_DATA_SIZE) {
            throw new IllegalArgumentException("invalid data size");
        }
        channel.position((long) id * BLOCK_REAL_SIZE);
        data.position(0);
        data.limit(BLOCK_DATA_SIZE);

        if(encrypted) {
            ByteBuffer encBuff = ByteBuffer.allocate(BLOCK_REAL_SIZE);
            channel.read(encBuff);

            byte[] array = decrypt(encBuff.array());
            data.put(array);
        }
        else {
            channel.read(data);
        }
        data.flip();
    }

    @Override
    public void write(int id, ByteBuffer data) throws IOException {
        if(id <= 0 || id > count()) {
            throw new IllegalArgumentException("invalid block id " + id);
        }
        if(data.capacity() > BLOCK_DATA_SIZE) {
            throw new IllegalArgumentException("invalid data size");
        }
        channel.position((long) id * BLOCK_REAL_SIZE);

        data.position(0);
        data.limit(BLOCK_DATA_SIZE);

        if(encrypted) {
            byte[] array = new byte[BLOCK_DATA_SIZE];
            data.get(array);

            ByteBuffer encBuff = ByteBuffer.allocate(BLOCK_REAL_SIZE);
            encBuff.put(encrypt(array));
            encBuff.flip();

            channel.write(encBuff);
        }
        else {
            channel.write(data);
        }

        data.flip();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    void create(Path file, SecurityOptions secOpts) throws IOException {
        Set<OpenOption> optionSet = openOptions();
        optionSet.add(StandardOpenOption.CREATE);
        channel = FileChannel.open(file, optionSet);
        setupSecurity(secOpts);
        writeInitData();

        headData = ByteBuffer.allocate(HEAD_DATA_SIZE);
        writeHeader();
    }

    void open(Path file, SecurityOptions secOpts) throws IOException {
        channel = FileChannel.open(file, openOptions());
        setupSecurity(secOpts);

        readInitData();
        headData = ByteBuffer.allocate(HEAD_DATA_SIZE);
        readHeader();
    }

    private void setupSecurity(SecurityOptions secOpts) throws IOException {
        encrypted = secOpts != null
                && secOpts.password() != null
                && !secOpts.password().isEmpty();
        if(encrypted) {
            options = secOpts;
            encCipher = createCipher(Cipher.ENCRYPT_MODE);
            decCipher = createCipher(Cipher.DECRYPT_MODE);
        }
    }

    private Cipher createCipher(int mode) throws IOException {
        try {
            byte[] iv = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
            IvParameterSpec ivspec = new IvParameterSpec(iv);

            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            KeySpec spec = new PBEKeySpec(options.password().toCharArray(), options.salt().getBytes(), 65536, 256);
            SecretKey tmp = factory.generateSecret(spec);
            SecretKeySpec secretKey = new SecretKeySpec(tmp.getEncoded(), "AES");

            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
            cipher.init(mode, secretKey, ivspec);

            return cipher;
        }
        catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private Set<OpenOption> openOptions() {
        Set<OpenOption> optionSet = new HashSet<>();
        optionSet.add(StandardOpenOption.READ);
        optionSet.add(StandardOpenOption.WRITE);
        return optionSet;
    }

    private void writeHeader() throws IOException {
        channel.position(INIT_DATA_SIZE);
        headData.position(0);
        if(encrypted) {
            byte[] encrypted = encrypt(headData.array());
            channel.write(ByteBuffer.wrap(encrypted));
        }
        else {
            channel.write(headData);
        }
    }

    private void readHeader() throws IOException {
        channel.position(INIT_DATA_SIZE);
        headData.position(0);
        if(encrypted) {
            ByteBuffer encData = ByteBuffer.allocate(HEAD_REAL_SIZE);
            channel.read(encData);
            headData.put(decrypt(encData.array()));
        }
        else {
            channel.read(headData);
        }
    }

    private void writeInitData() throws IOException {
        short flags = 0;
        if(encrypted) {
            flags |= (short) 1;
        }

        ByteBuffer initData = ByteBuffer.allocate(INIT_DATA_SIZE);
        initData.put(TITLE);
        initData.putShort(VERSION);
        initData.putShort(flags);
        initData.flip();
        channel.position(0);
        channel.write(initData);
    }

    private void readInitData() throws IOException {
        ByteBuffer initData = ByteBuffer.allocate(INIT_DATA_SIZE);
        channel.position(0);
        channel.read(initData);
        initData.flip();

        byte[] title = new byte[TITLE.length];
        initData.get(title);
        if(!Arrays.equals(title, TITLE)) {
            throw new IOException("invalid file");
        }

        if(initData.getShort() != VERSION) {
            throw new IOException("invalid version");
        }

        short flags = initData.getShort();
        boolean isEncrypted = (flags & 1) != 0;
        if(isEncrypted != encrypted) {
            throw new IOException("invalid password");
        }
    }

    private byte[] encrypt(byte[] array) throws IOException {
        try {
            return encCipher.doFinal(array);
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new IOException(e);
        }
    }

    private byte[] decrypt(byte[] array) throws IOException {
        try {
            return decCipher.doFinal(array);
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new IOException(e);
        }
    }
}

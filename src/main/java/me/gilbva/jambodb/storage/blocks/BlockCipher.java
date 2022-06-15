package me.gilbva.jambodb.storage.blocks;

import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;

public class BlockCipher {
    private static final String SALT = "ssshhhhhhhhhhh!!!!";

    private Cipher encrypt;

    private Cipher decrypt;

    public BlockCipher(String password) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidAlgorithmParameterException, InvalidKeyException, NoSuchPaddingException {
        encrypt = createCipher(password, Cipher.ENCRYPT_MODE);
        decrypt = createCipher(password, Cipher.DECRYPT_MODE);
    }

    Cipher createCipher(String password, int mode) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidAlgorithmParameterException, InvalidKeyException, NoSuchPaddingException {
        byte[] iv = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        IvParameterSpec ivspec = new IvParameterSpec(iv);

        SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        KeySpec spec = new PBEKeySpec(password.toCharArray(), SALT.getBytes(), 65536, 256);
        SecretKey tmp = factory.generateSecret(spec);
        SecretKeySpec secretKey = new SecretKeySpec(tmp.getEncoded(), "AES");

        Cipher result = Cipher.getInstance("AES/CBC/PKCS5PADDING");
        result.init(mode, secretKey, ivspec);
        return result;
    }

    public byte[] encrypt(byte[] data) throws IllegalBlockSizeException, BadPaddingException {
        if(data.length > BlockStorage.BLOCK_SIZE) {
            throw new IllegalBlockSizeException();
        }

        return encrypt.doFinal(data);
    }

    public byte[] decrypt(byte[] data) throws IllegalBlockSizeException, BadPaddingException {
        if(data.length > JamboBlksV1.INTERNAL_BLOCK_SIZE) {
            throw new IllegalBlockSizeException();
        }

        return decrypt.doFinal(data);
    }
}

package me.gilbva.jambodb.storage.blocks;

import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;

public class BlockCipher {
    private Cipher encrypt;

    private Cipher decrypt;

    public BlockCipher(SecurityOptions opts) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidAlgorithmParameterException, InvalidKeyException, NoSuchPaddingException {
        encrypt = createCipher(opts, Cipher.ENCRYPT_MODE);
        decrypt = createCipher(opts, Cipher.DECRYPT_MODE);
    }

    public Cipher createCipher(SecurityOptions opts, int mode) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidAlgorithmParameterException, InvalidKeyException, NoSuchPaddingException {
        byte[] iv = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        IvParameterSpec ivspec = new IvParameterSpec(iv);

        SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        KeySpec spec = new PBEKeySpec(opts.password().toCharArray(), opts.salt().getBytes(), 65536, 256);
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

package com.github.jambodb.storage.types;

import com.github.jambodb.storage.btrees.Serializer;

import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Serializer for java.math.BigInteger.
 */
public class BigIntegerSerializer implements Serializer<BigInteger> {
    public static final BigIntegerSerializer INSTANCE = new BigIntegerSerializer();

    @Override
    public int size(ByteBuffer buffer) {
        return buffer.get()+1;
    }

    @Override
    public int size(BigInteger value) {
        return value.toByteArray().length+1;
    }

    @Override
    public BigInteger read(ByteBuffer buffer) {
        int size = buffer.get();
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return new BigInteger(bytes);
    }

    @Override
    public void write(ByteBuffer buffer, BigInteger value) {
        byte[] bytes = value.toByteArray();
        buffer.put((byte) (bytes.length));
        buffer.put(bytes);
    }
}

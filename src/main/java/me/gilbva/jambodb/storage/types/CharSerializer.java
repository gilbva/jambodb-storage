package me.gilbva.jambodb.storage.types;

import me.gilbva.jambodb.storage.btrees.Serializer;

import java.nio.ByteBuffer;

/**
 * Serializer for java.lang.Char.
 */
public class CharSerializer implements Serializer<Character> {
    public static final CharSerializer INSTANCE = new CharSerializer();

    @Override
    public int size(ByteBuffer buffer) {
        return 2;
    }

    @Override
    public int size(Character value) {
        return 2;
    }

    @Override
    public Character read(ByteBuffer buffer) {
        return buffer.getChar();
    }

    @Override
    public void write(ByteBuffer buffer, Character value) {
        buffer.putChar(value);
    }
}

package com.github.jambodb.storage.utils;

import com.github.jambodb.storage.btrees.Serializer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class SerializableWrapperList<T> {
    private List<byte[]> array;
    private transient List<T> list;
    private transient Serializer<T> serializer;

    public SerializableWrapperList() {
    }

    public SerializableWrapperList(Serializer<T> serializer) {
        list = new LinkedList<>();
        this.serializer = serializer;
    }

    public void prepare(int blockSize) {
        array = null;
        if (list != null && !list.isEmpty()) {
            array = new ArrayList<>(list.size());
            for (T item : list) {
                ByteBuffer buffer = ByteBuffer.allocate(blockSize);
                serializer.write(item, buffer);
                buffer.flip();
                array.add(buffer.array());
            }
        }
    }

    public void sync(Serializer<T> serializer) {
        this.serializer = serializer;
        list = new LinkedList<>();
        if (array != null && !array.isEmpty()) {
            for (byte[] bytes : array) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                list.add(serializer.read(buffer));
            }
        }
        array = null;
    }

    public T get(int index) {
        if (list != null && index < list.size()) {
            return list.get(index);
        }
        return null;
    }

    public void put(int index, T key) {
        if (list == null) {
            list = new LinkedList<>();
        }
        while (index > list.size()) {
            list.add(null);
        }
        list.add(index, key);
    }
}

package com.github.jambodb.storage.utils;

import com.github.jambodb.storage.btrees.Serializer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class SerializableList<T> implements Iterable<T> {
    private List<T> list;
    private final Serializer<T> serializer;

    public SerializableList(Serializer<T> serializer) {
        this.serializer = serializer;
    }

    public T get(int index) {
        if (list != null && index < list.size()) {
            return list.get(index);
        }
        return null;
    }

    private byte[] getBytes(int index) {
        if (list != null && index < list.size()) {
            return serializer.serialize(list.get(index));
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

    public void add(byte[] bytes) {
        if (bytes != null) {
            if (list == null) {
                list = new LinkedList<>();
            }
            list.add(serializer.parse(bytes));
        }
    }

    public int size() {
        return list != null ? list.size() : 0;
    }

    @Override
    public Iterator<T> iterator() {
        if (list == null) {
            list = new LinkedList<>();
        }
        return list.iterator();
    }

    public Iterator<byte[]> bytesIterator() {
        return new ByteIterator(this);
    }

    private class ByteIterator implements Iterator<byte[]> {
        private final SerializableList<T> list;
        private int index;

        public ByteIterator(SerializableList<T> serializableList) {
            this.list = serializableList;
            index = 0;
        }

        @Override
        public boolean hasNext() {
            return index < list.size();
        }

        @Override
        public byte[] next() {
            if (index < list.size()) {
                return list.getBytes(index++);
            }
            return null;
        }
    }
}

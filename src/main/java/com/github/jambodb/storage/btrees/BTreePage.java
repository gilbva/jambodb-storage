package com.github.jambodb.storage.btrees;

public interface BTreePage<K, V> {
    int id();

    int size();

    void size(int size);

    boolean isLeaf();

    boolean isFull();

    boolean isHalf();

    boolean canBorrow();

    K key(int index);

    void key(int index, K key);

    V value(int index);

    void value(int index, V value);

    int child(int index);

    void child(int index, int child);
}

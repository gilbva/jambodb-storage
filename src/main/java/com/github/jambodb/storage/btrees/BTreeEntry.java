package com.github.jambodb.storage.btrees;

public interface BTreeEntry<K, V> {
    K key();

    V value();
}
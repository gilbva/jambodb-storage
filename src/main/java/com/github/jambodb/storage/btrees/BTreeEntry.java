package com.github.jambodb.storage.btrees;

/**
 * An entry of a BTree
 *
 * @param <K> the type for the key.
 * @param <V> the type for the value.
 */
public interface BTreeEntry<K, V> {
    /**
     * Gets the key for this entry.
     *
     * @return the key for this entry.
     */
    K key();

    /**
     * Gets the value for this entry.
     *
     * @return the value for this entry.
     */
    V value();
}
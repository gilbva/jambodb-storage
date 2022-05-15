package com.github.jambodb.storage.btrees;

/**
 * This interface represents a page of a BTree.
 *
 * @param <K> the type for the key.
 * @param <V> the type for the value.
 */
public interface BTreePage<K, V> {
    /**
     * The numeric id of this page.
     *
     * @return an int value representing the id of this page.
     */
    int id();

    /**
     * Gets the current amount of elements present in this page.
     *
     * @return an int representing the amount of elements in this page.
     */
    int size();

    /**
     * Sets the current size of the page.
     *
     * @param value the size of the page, it must be less than its total capacity.
     */
    void size(int value);

    /**
     * Determines if this page is a leaf page or not.
     *
     * @return true if this page is a leaf page.
     */
    boolean isLeaf();

    /**
     * Determines if the page has exceeded its capacity for holding new elements.
     *
     * @return true the page is full, false otherwise.
     */
    boolean isFull();

    /**
     * Determines if the page has less than half of its capacity.
     *
     * @return true the page is half empty, false otherwise.
     */
    boolean isHalf();

    /**
     * Determines if the page can borrow one element to a sibling page.
     *
     * @return true if at least one element can be borrowed, false otherwise.
     */
    boolean canBorrow();

    /**
     * Gets the key for the element at the given index.
     *
     * @param index the index of the element to look for.
     * @return the key of the given element.
     */
    K key(int index);

    /**
     * Sets the key for the element at the given index.
     *
     * @param index the index of the element to set the key to.
     * @param data the key of the given element.
     */
    void key(int index, K data);

    /**
     * Gets the value for the element at the given index.
     *
     * @param index the index of the element to look for.
     * @return the value of the given element.
     */
    V value(int index);

    /**
     * Swaps the ith element with the jth element.
     *
     * @param i the first element.
     * @param j the second element.
     */
    void swap(int i, int j);

    /**
     * Sets the value for the element at the given index.
     *
     * @param index the index of the element to look for.
     * @param data the value of the given element.
     */
    void value(int index, V data);

    /**
     * Gets the id of the child page for the element at the given index.
     *
     * @param index the index of the element to look for.
     * @return the id of the child page for the given element.
     */
    int child(int index);

    /**
     * Sets the id of the child page for the element at the given index.
     *
     * @param index the index of the element to look for.
     * @param id the id of the child page for the given element.
     */
    void child(int index, int id);
}

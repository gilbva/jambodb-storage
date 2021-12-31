package com.github.jambodb.storage.btrees;

import java.io.IOException;
import java.util.*;

/**
 * This class represents a generic BTree implementation, it provides
 * the basic algorithms involve in reading and writing values to the given
 * page storage. No assumption is made about the underlying storage, the pages
 * managed by this structure could be on memory, disk or even the network.
 * The only requirement is that the pages can be created and the cells can be copied
 * and moved at will.
 *
 * @param <K> The type for the key.
 * @param <V> The type for the value.
 */
public final class BTree<K extends Comparable<K>, V> {

    /**
     * This class holds a reference to a particular point in the tree,
     * a node (or cell) is located by its page and its index within that page.
     *
     * @param <K> The type for the key.
     * @param <V> The type for the value.
     */
    static class Node<K, V> implements BTreeEntry<K, V> {
        BTreePage<K, V> page;
        int index;

        public Node(BTreePage<K, V> page, int index) {
            this.page = page;
            this.index = index;
        }

        @Override
        public K key() {
            return page.key(index);
        }

        @Override
        public V value() {
            return page.value(index);
        }

        @Override
        public String toString() {
            return String.format("%s=%s", key(), value());
        }
    }

    /**
     * The result of a lookup operation perform on the tree, this class
     * holds the last node visited by the lookup method and if the actual key
     * was found or not.
     *
     * @param <K> The type for the key.
     * @param <V> The type for the value.
     */
    static class Result<K, V> extends Node<K, V> {
        boolean found;

        public Result(BTreePage<K, V> page, int index, boolean found) {
            super(page, index);
            this.found = found;
        }
    }

    /**
     * This is the reference to the underlying page storage.
     */
    Pager<BTreePage<K, V>> pager;

    /**
     * A reference to the root page of the tree.
     */
    BTreePage<K, V> root;

    /**
     * The constructor builds a BTree instance for the given page storage.
     *
     * @param pager The object responsible for storing the pages managed by this tree.
     * @throws IOException Thrown by the underlying storage.
     */
    public BTree(Pager<BTreePage<K, V>> pager) throws IOException {
        this.pager = pager;
        this.root = pager.page(pager.root());
    }

    /**
     * Given a key this method will search the tree from top to bottom to find it,
     * if the given key is found this method will read and return its value but if the
     * given key is not found the method will return null.
     *
     * IMPORTANT: if the underlying storage returns null for the value of the given key,
     * this method will still return null, hence by using this method along there is
     * no way of determining if a given key does not exist or its value is null.
     *
     * @param key The key to look for in the tree.
     * @return The value associated with the key or null if it does not exist.
     * @throws IOException Thrown by the underlying storage.
     */
    public V get(K key) throws IOException {
        var result = lookup(key, null);
        if(result.found) {
            return result.page.value(result.index);
        }
        return null;
    }

    /**
     * Sets the given key to the given value by inserting or updating it.
     *
     * @param key
     * @param value
     * @throws IOException
     */
    public void put(K key, V value) throws IOException {
        var ancestors = new LinkedList<Node<K, V>>();
        var result = lookup(key, ancestors);
        if(result.found) {
            result.page.value(result.index, value);
        }
        else {
            insertPlace(result.page, result.index);
            result.page.key(result.index, key);
            result.page.value(result.index, value);

            if(result.page.isFull()) {
                split(result.page, ancestors);
            }
        }
    }

    /**
     * Removes the given key from the tree if it exists.
     *
     * @param key
     * @throws IOException
     */
    public void remove(K key) throws IOException {
        var ancestors = new LinkedList<Node<K, V>>();
        var result = lookup(key, ancestors);
        if(result.found) {
            deletePlace(result.page, result.index);

            if(result.page.isHalf()) {
                fill(result.page, ancestors);
            }
        }
    }

    /**
     * Returns a range of entries from the given start key (inclusive) to the given
     * end key (also inclusive)
     *
     * @param from
     * @param to
     * @return
     * @throws IOException
     */
    public Iterator<BTreeEntry<K, V>> query(K from, K to) throws IOException {
        if(root.isLeaf() && root.size() == 0) {
            return Collections.emptyIterator();
        }

        final Deque<Node<K, V>> ancestors = new LinkedList<>();
        final Node<K, V> fromNode = from == null ? first(root, ancestors) : lookupFirst(from, ancestors);

        return new Iterator<>() {
            Node<K, V> current = fromNode;

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public BTreeEntry<K, V> next() {
                var entry = current;
                try {
                    current = BTree.this.next(current, ancestors);
                    if(current != null) {
                        var key = current.key();
                        if (to != null && key.compareTo(to) > 0) {
                            current = null;
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return entry;
            }
        };
    }

    /**
     * Gets the first node of the sub-tree rooted at the given current node.
     *
     * @param current
     * @param ancestors
     * @return
     * @throws IOException
     */
    Node<K, V> first(BTreePage<K, V> current, Deque<Node<K, V>> ancestors) throws IOException {
        while(!current.isLeaf()) {
            if(ancestors != null) {
                ancestors.addFirst(new Node<>(current, 0));
            }
            current = getChildPage(current, 0);
        }
        if(current.size() > 0) {
            return new Node<>(current, 0);
        }
        return null;
    }

    /**
     * Gets the last node of the sub-tree rooted at the given current node.
     *
     * @param current
     * @param ancestors
     * @return
     * @throws IOException
     */
    Node<K, V> last(BTreePage<K, V> current, Deque<Node<K, V>> ancestors) throws IOException {
        while(!current.isLeaf()) {
            if(ancestors != null) {
                ancestors.addFirst(new Node<>(current, 0));
            }
            current = getChildPage(current, current.size());
        }
        if(current.size() > 0) {
            return new Node<>(current, current.size());
        }
        return null;
    }

    /**
     * Returns the next node from the given current node.
     *
     * @param current
     * @param ancestors
     * @return
     * @throws IOException
     */
    Node<K, V> next(Node<K, V> current, Deque<Node<K, V>> ancestors) throws IOException {
        if(current.page.isLeaf()) {
            if(current.index+1 < current.page.size()) {
                return new Node<>(current.page, current.index+1);
            }
        }
        else {
            if(current.index < current.page.size()) {
                ancestors.addFirst(new Node<>(current.page, current.index+1));
                return first(getChildPage(current.page, current.index+1), ancestors);
            }
        }

        while(!ancestors.isEmpty()) {
            var node = ancestors.removeFirst();
            if(node.index < node.page.size()) {
                return node;
            }
        }
        return null;
    }

    /**
     * Returns the previous node from the given current node.
     *
     * @param current
     * @param ancestors
     * @return
     * @throws IOException
     */
    Node<K, V> prev(Node<K, V> current, Deque<Node<K, V>> ancestors) throws IOException {
        if(current.page.isLeaf()) {
            if(current.index-1 >= 0) {
                return new Node<>(current.page, current.index-1);
            }
        }
        else {
            if(current.index >= 0) {
                ancestors.addFirst(current);
                return last(getChildPage(current.page, current.index), ancestors);
            }
        }

        while(!ancestors.isEmpty()) {
            var node = ancestors.removeFirst();
            if(node.index-1 >= 0) {
                return new Node<>(node.page, node.index-1);
            }
        }
        return null;
    }

    /**
     * Recursively search for the specified key starting at the root of the tree, until either the key is found,
     * or a leaf node that does not contain the key is reached.
     *
     * @param key
     * @param ancestors
     * @return
     * @throws IOException
     */
    Result<K, V> lookup(K key, Deque<Node<K, V>> ancestors) throws IOException {
        var current = root;
        while (!current.isLeaf()) {
            var result = search(current, key);
            if(result.found) {
                return result;
            }

            current = getChildPage(result.page, result.index);
            if(ancestors != null && !current.isLeaf()) {
                ancestors.addFirst(result);
            }
        }

        return search(current, key);
    }

    Node<K, V> lookupFirst(K key, Deque<Node<K, V>> ancestors) throws IOException {
        var result = lookup(key, ancestors);

        if(!result.found) {
            var prev = prev(result, ancestors);
            if(prev != null && prev.key().compareTo(key) >= 0) {
                return prev;
            }

            if(result.key() != null && result.key().compareTo(key) >= 0) {
                return result;
            }

            return next(result, ancestors);
        }

        return result;
    }

    /**
     * Performs a binary search for the specified key in the given page.
     *
     * @param page
     * @param key
     * @return
     * @throws IOException
     */
    Result<K, V> search(BTreePage<K,V> page, K key) throws IOException {
        int p = 0;
        int r = page.size() - 1;

        while (p <= r) {
            int q = p + (r - p) / 2;

            var currentKey = page.key(q);
            if(Objects.equals(currentKey, key)) {
                return new Result<>(page, q, true);
            }

            if(key.compareTo(currentKey) <= 0) {
                r = q - 1;
            }
            else {
                p = q + 1;
            }
        }

        return new Result<>(page, p, false);
    }

    /**
     * Splits a page that has become full into two pages by creating a new page and moving half
     * of the nodes to the new page.
     *
     * @param source
     * @param ancestors
     * @throws IOException
     */
    void split(BTreePage<K, V> source, Deque<Node<K, V>> ancestors) throws IOException {
        Node<K, V> parent;
        if(ancestors.isEmpty()) {
            parent = grow();
        }
        else {
            parent = ancestors.removeFirst();
        }

        int mid = source.size() / 2;
        var target = pager.create(source.isLeaf());
        move(source, target, mid + 1);
        promoteLast(source, parent);
        parent.page.child(parent.index, source.id());
        parent.page.child(parent.index+1, target.id());

        if(parent.page.isFull()) {
            split(parent.page, ancestors);
        }
    }

    /**
     * Fills a page by borrowing from or merging it with either page to its side.
     *
     * @param target
     * @param ancestors
     * @throws IOException
     */
    void fill(BTreePage<K, V> target, Deque<Node<K, V>> ancestors) throws IOException {
        if(ancestors.isEmpty()) {
            shrink();
            return;
        }

        var parent = ancestors.removeFirst();
        if(borrowLeft(parent, target) || borrowRight(parent, target)) {
            return;
        }

        if (mergeLeft(parent, target) || mergeRight(parent, target)) {
            if (parent.page.isHalf()) {
                fill(parent.page, ancestors);
            }
            return;
        }

        throw new IllegalStateException("Could not fill the node.");
    }

    /**
     * Grows the tree by adding a new root page.
     *
     * @return
     * @throws IOException
     */
    Node<K, V> grow() throws IOException {
        var newRoot = pager.create(false);
        newRoot.child(0, root.id());
        root = newRoot;
        pager.root(root.id());
        return new Node<>(root, 0);
    }

    /**
     * Shrinks the tree by removing the root page.
     *
     * @throws IOException
     */
    void shrink() throws IOException {
        if(root.size() == 0 && !root.isLeaf()) {
            pager.remove(root.id());
            root = getChildPage(root, 0);
            pager.root(root.id());
        }
    }

    /**
     * Try to borrow the last node of the page to the left of the target page.
     *
     * @param parent
     * @param target
     * @return
     * @throws IOException
     */
    boolean borrowLeft(Node<K, V> parent, BTreePage<K, V> target) throws IOException {
        if(parent.index <= 0) {
            return false;
        }

        var source = getChildPage(parent.page, parent.index);
        if(!source.canBorrow()) {
            return false;
        }

        rotateRight(parent, source, target);
        return true;
    }

    /**
     * Try to borrow the first node of the page to the right of the target page.
     *
     * @param parent
     * @param target
     * @return
     * @throws IOException
     */
    boolean borrowRight(Node<K, V> parent, BTreePage<K, V> target) throws IOException {
        if(parent.index >= parent.page.size()) {
            return false;
        }

        var source = getChildPage(parent.page, parent.index+1);
        if(!source.canBorrow()) {
            return false;
        }

        rotateLeft(parent, source, target);
        return true;
    }

    /**
     * Merges the target page with the page to the left
     *
     * @param parent
     * @param source
     * @return
     * @throws IOException
     */
    boolean mergeLeft(Node<K, V> parent, BTreePage<K, V> source) throws IOException {
        if(parent.index <= 0) {
            return false;
        }

        parent.index--;
        var target = getChildPage(parent.page, parent.index);
        merge(parent, source, target);
        return true;
    }

    /**
     * Merges the target page with the page to the right.
     *
     * @param parent
     * @param target
     * @return
     * @throws IOException
     */
    boolean mergeRight(Node<K, V> parent, BTreePage<K, V> target) throws IOException {
        if(parent.index >= parent.page.size()) {
            return false;
        }

        var source = getChildPage(parent.page, parent.index+1);
        merge(parent, source, target);
        return true;
    }

    /**
     * This method merges two pages by moving the parent node along with all the nodes of the source page
     * to the target page. the method also deletes the source page from the pager.
     *
     * @param parent the parent node of the target page.
     * @param source the page to the right of the parent node.
     * @param target the page to the left of the parent node.
     * @throws IOException if any I/O error occurs deleting the source page.
     */
    void merge(Node<K, V> parent, BTreePage<K, V> source, BTreePage<K, V> target) throws IOException {
        target.size(target.size()+1);
        target.key(target.size()-1 , parent.page.key(parent.index));
        target.value(target.size()-1 , parent.page.value(parent.index));

        deletePlace(parent.page, parent.index);
        parent.page.child(parent.index, target.id());

        move(source, target, 0);
        pager.remove(source.id());
    }

    /**
     * Move the given node and all the following from the source page to the end of the target page.
     * the elements will be removed from the source page except for the child of the element at the given index.
     *
     * @param source the page holding the elements to move, this should be a non-empty page.
     * @param target the target page to move the elements to, this page can be empty.
     *               if the page is not empty the nodes will be put at the end of this page.
     * @param index a valid index at the source page signaling the first element that will be moved.
     */
    void move(BTreePage<K, V> source, BTreePage<K, V> target, int index) {
        if(index < 0 || index > source.size()) {
            throw new IndexOutOfBoundsException(String.format("index %d is out of bounds with respect to the page %d with size %d", index, source.id(), source.size()));
        }
        int prevSize = target.size();
        int moveSize = source.size() - index;
        target.size(target.size() + moveSize);
        for(int i = 0; i <= moveSize; i++) {
            if(i < moveSize) {
                target.key(prevSize + i, source.key(index + i));
                target.value(prevSize + i, source.value(index + i));
            }
            if(!target.isLeaf()) {
                target.child(prevSize + i, source.child(index + i));
            }
        }
        source.size(source.size() - moveSize);
    }

    /**
     * Promotes the last node of the source page into the parent node.
     * This method will insert a new place at the parent node and will move all the
     * children one step to the right, but it will not touch the children at the source page.
     *
     * @param source The source page to get the element to be promoted, it must be the child of the parent node.
     * @param parent The parent node where the promoted element will be place.
     */
    void promoteLast(BTreePage<K, V> source, Node<K, V> parent) {
        if(source.size() <= 0) {
            throw new IllegalArgumentException(String.format("the page %d is empty", source.id()));
        }
        insertPlace(parent.page, parent.index);
        parent.page.key(parent.index, source.key(source.size()-1));
        parent.page.value(parent.index, source.value(source.size()-1));
        source.size(source.size()-1);
    }

    /**
     * Performs a left rotation by inserting the parent node at the end of the target page
     * and then moving the first node of the source page to the parent node.
     *
     * @param parent The parent node (page and index) that represents the pivot of the rotation.
     * @param source The source page, must be the right child of the parent node.
     * @param target The target page, must be the left child of the parent node.
     */
    void rotateLeft(Node<K, V> parent, BTreePage<K, V> source, BTreePage<K, V> target) {
        if(source.size() <= 0) {
            throw new IllegalArgumentException(String.format("the page %d is empty", source.id()));
        }
        if(parent.index < 0 || parent.index >= parent.page.size()) {
            throw new IndexOutOfBoundsException(String.format("index %d is out of bounds with respect to the page %d with size %d", parent.index, parent.page.id(), parent.page.size()));
        }
        target.size(target.size()+1);
        target.key(target.size()-1, parent.page.key(parent.index));
        target.value(target.size()-1, parent.page.value(parent.index));
        if(!target.isLeaf()) {
            target.child(target.size(), source.child(0));
        }

        parent.page.key(parent.index, source.key(0));
        parent.page.value(parent.index, source.value(0));
        deletePlace(source, 0);
    }

    /**
     * Performs a right rotation by inserting the parent node at the beginning of the target page
     * and then moving the last node of the source page to the parent node.
     *
     * @param parent The parent node (page and index) that represents the pivot of the rotation.
     * @param source The source page, must be the left child of the parent node.
     * @param target The target page, must be the right child of the parent node.
     */
    void rotateRight(Node<K, V> parent, BTreePage<K, V> source, BTreePage<K, V> target) {
        if(source.size() <= 0) {
            throw new IllegalArgumentException(String.format("the page %d is empty", source.id()));
        }
        if(parent.index < 0 || parent.index >= parent.page.size()) {
            throw new IndexOutOfBoundsException(String.format("index %d is out of bounds with respect to the page %d with size %d", parent.index, parent.page.id(), parent.page.size()));
        }
        insertPlace(target, 0);
        target.key(0, parent.page.key(parent.index));
        target.value(0, parent.page.value(parent.index));
        if(!target.isLeaf()) {
            target.child(0, source.child(source.size()));
        }

        parent.page.key(parent.index, source.key(source.size()-1));
        parent.page.value(parent.index, source.value(source.size()-1));
        source.size(source.size()-1);
    }

    /**
     * This method inserts an empty spot at the given index by moving
     * every node one step to the right.
     *
     * @param page The page to insert the place into.
     * @param index The index at which the empty spot will be inserted.
     */
    void insertPlace(BTreePage<K, V> page, int index) {
        if(index < 0 || index > page.size()) {
            throw new IndexOutOfBoundsException(String.format("index %d is out of bounds with respect to the page %d with size %d", index, page.id(), page.size()));
        }

        page.size(page.size()+1);
        for(int i = page.size(); i > index; i--) {
            if(i < page.size()) {
                page.key(i, page.key(i-1));
                page.value(i, page.value(i-1));
            }
            if(!page.isLeaf()) {
                page.child(i, page.child(i-1));
            }
        }
    }

    /**
     * This method will delete the node at the given index by moving all
     * the remaining values one step left.
     *
     * @param page The page to delete the node from.
     * @param index The index to be deleted from the given page.
     */
    void deletePlace(BTreePage<K, V> page, int index) {
        if(index < 0 || index >= page.size()) {
            throw new IndexOutOfBoundsException(String.format("index %d is out of bounds with respect to the page %d with size %d", index, page.id(), page.size()));
        }

        for(int i = index; i < page.size(); i++) {
            if(i < page.size() - 1) {
                page.key(i, page.key(i+1));
                page.value(i, page.value(i+1));
            }
            if(!page.isLeaf()) {
                page.child(i, page.child(i+1));
            }
        }
        page.size(page.size()-1);
    }

    /**
     * Gets the child page referenced by the specified node, this method will invoke the underlying pager.page()
     * method to load the child page.
     *
     * @param page The non-leaf page to get the child id from.
     * @param index The valid index withing the bounds of page that holds the id of the child page.
     * @return The page object referenced by the parent page at the given index.
     * @throws IOException thrown by the Pager interface if any I/O errors occur loading the page.
     * @throws IllegalArgumentException if the page parameter is not a leaf page.
     * @throws IndexOutOfBoundsException if the page parameter is not a leaf page.
     */
    BTreePage<K,V> getChildPage(BTreePage<K,V> page, int index) throws IOException {
        if(page.isLeaf()) {
            throw new IllegalArgumentException("getChildPage called on leaf page.");
        }
        if(index < 0 || index > page.size()) {
            throw new IndexOutOfBoundsException(String.format("index %d is out of bounds with respect to the page %d with size %d", index, page.id(), page.size()));
        }
        return pager.page(page.child(index));
    }
}

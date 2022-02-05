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
     * While the class is mainly for internal use only, it implements the
     * BTreeEntry interface which is the interface used to gain access to a
     * particular element in the tree from the outside. Along with the convenient access
     * to the key and value of the node, the class offer also some utility
     * methods to test if the node is off-page, to provide access to the child of the
     * node, and to test if a particular key is less or greater than the key of this node.
     *
     * @param <K> The type for the key.
     * @param <V> The type for the value.
     */
    static class Node<K extends Comparable<K>, V> implements BTreeEntry<K, V> {
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

        /**
         * Gets the id of the child for this element.
         *
         * @return the id of the child stored in the element pointed by this node.
         */
        public int child() {
            return page.child(index);
        }

        @Override
        public String toString() {
            return String.format("%s=%s", key(), value());
        }

        /**
         * Determines if this node is off-page, meaning that
         * his index is equal or greater than the size of the page.
         *
         * @return true if the node is outside the page, false if
         * the node points to an element inside the page bounds.
         */
        public boolean isOffPage() {
            return index >= page.size();
        }

        /**
         * Determines if the key of this node is less than
         * the given key.
         *
         * @param key The key to compare with.
         * @return true if this node's key is less than the one given.
         * false otherwise.
         */
        public boolean lesserThan(K key) {
            return key().compareTo(key) < 0;
        }

        /**
         * Determines if the key of this node is greater than
         * the given key.
         *
         * @param key The key to compare with.
         * @return true if this node's key is greater than the one given.
         * false otherwise.
         */
        public boolean greaterThan(K key) {
            return key().compareTo(key) > 0;
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
    static class Result<K extends Comparable<K>, V> extends Node<K, V> {
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
        if (this.root == null) {
            this.root = pager.create(true);
            pager.root(this.root.id());
        }
    }

    /**
     * Gets the underlying pager for this BTree.
     *
     * @return the underlying pager.
     */
    public Pager<BTreePage<K, V>> getPager() {
        return pager;
    }

    /**
     * Given a key this method will search the tree from top to bottom to find it,
     * if the given key is found this method will read and return its value but if the
     * given key is not found the method will return null.
     *
     * @param key The key to look for in the tree.
     * @return The value associated with the key or null if it does not exist.
     * @throws IOException Thrown by the underlying storage.
     */
    public V get(K key) throws IOException {
        var result = lookup(key, null);
        if (result.found) {
            return result.page.value(result.index);
        }
        return null;
    }

    /**
     * Determines if a particular key exists in the tree,
     * by searching from top to bottom.
     *
     * @param key the key to lookup.
     * @return true if the key exists, false if the key does not exist.
     * @throws IOException thrown by the Pager interface if any I/O errors occur.
     */
    public boolean exists(K key) throws IOException {
        var result = lookup(key, null);
        return result.found;
    }

    /**
     * Sets the given key to the given value by inserting or updating it.
     *
     * @param key   the key to insert or update in the tree.
     * @param value the value to be inserted or updated for the given key.
     * @throws IOException thrown by the Pager interface if any I/O errors occur.
     */
    public void put(K key, V value) throws IOException {
        var ancestors = new LinkedList<Node<K, V>>();
        var result = lookup(key, ancestors);
        if (result.found) {
            result.page.value(result.index, value);
        } else {
            insertPlace(result.page, result.index);
            result.page.key(result.index, key);
            result.page.value(result.index, value);

            if (result.page.isFull()) {
                split(result.page, ancestors);
            }
        }
    }

    /**
     * Removes the given key from the tree if it exists.
     *
     * @param key the key to be removed from the tree.
     * @throws IOException thrown by the Pager interface if any I/O errors occur.
     */
    public void remove(K key) throws IOException {
        var ancestors = new LinkedList<Node<K, V>>();
        var result = lookup(key, ancestors);
        if (result.found) {
            deletePlace(result.page, result.index);

            if (result.page.isHalf()) {
                fill(result.page, ancestors);
            }
        }
    }

    /**
     * Returns a range of entries from the given start key (inclusive) to the given
     * end key (also inclusive)
     *
     * @param from the starting key (inclusive) for the query, if null, the first key of the tree will be taken.
     * @param to   the ending key (inclusive) for the query. if null the last key of the tree will be taken.
     * @return an iterator object that allows the user to iterate through the keys.
     * @throws IOException thrown by the Pager interface if any I/O errors occur.
     */
    public Iterator<BTreeEntry<K, V>> query(K from, K to) throws IOException {
        if (root.isLeaf() && root.size() == 0) {
            return Collections.emptyIterator();
        }

        final Deque<Node<K, V>> ancestors = new LinkedList<>();
        final Node<K, V> fromNode = startingNode(from, to, ancestors);

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
                    if (current != null) {
                        var key = current.key();
                        if (to != null && key.compareTo(to) > 0) {
                            current = null;
                        }
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
                return entry;
            }
        };
    }

    /**
     * This method localizes the starting node for the given query range, which is the node with
     * the lowest key that is equal or greater than the from key and lesser than the to key.
     *
     * @param from The lower bound for the query, the resulting node will be equal or greater than this key.
     * @param to The upper bound for the query, the resulting node will be equal or lesser than this key.
     * @param ancestors The queue to track the ancestors of the resulting node.
     * @return The starting node of the given range
     * @throws IOException thrown by the Pager interface if any I/O errors occur.
     */
    private Node<K, V> startingNode(K from, K to, Deque<Node<K, V>> ancestors) throws IOException {
        if(from == null) {
            return first(root, ancestors);
        }

        if(to != null && from.compareTo(to) > 0) {
            return null;
        }

        Node<K, V> first = lookupFirst(from, ancestors);
        if (to != null && first != null && first.greaterThan(to)) {
            first = null;
        }
        return first;
    }

    /**
     * Looks up the node for the smallest key that is greater than the given key.
     * this method returns the first node of the entire tree.
     *
     * @param from      the lower bound key to look for in the tree.
     * @param ancestors the list of ancestors that will be populated as the structure is searched from top to bottom.
     * @return a result node referencing the given key if found, or the node referencing the next key from the given key.
     * @throws IOException thrown by the Pager interface if any I/O errors occur.
     */
    Node<K, V> lookupFirst(K from, Deque<Node<K, V>> ancestors) throws IOException {
        var result = lookup(from, ancestors);
        if (result.found) {
            return result;
        }

        Node<K, V> current = result;
        if (current.isOffPage()) {
            current = prev(current, ancestors);
        }

        var prevAncestors = new LinkedList<>(ancestors);
        Node<K, V> prev = current;
        while (current != null && current.greaterThan(from)) {
            prevAncestors = new LinkedList<>(ancestors);
            prev = current;
            current = prev(current, ancestors);
        }

        ancestors.clear();
        ancestors.addAll(prevAncestors);
        current = prev;
        while (current != null) {
            if (current.greaterThan(from)) {
                return current;
            }
            current = next(current, ancestors);
        }

        return null;
    }

    /**
     * Gets the first node of the subtree rooted at the given current page.
     *
     * @param current   the root page of the subtree to look the first node for.
     * @param ancestors the list of ancestors for the current page.
     * @return the first node of the subtree rooted at the given page, or null if the page is null.
     * @throws IOException thrown by the Pager interface if any I/O errors occur.
     */
    Node<K, V> first(BTreePage<K, V> current, Deque<Node<K, V>> ancestors) throws IOException {
        while (!current.isLeaf()) {
            if (ancestors != null) {
                ancestors.addFirst(new Node<>(current, 0));
            }
            current = getChildPage(current, 0);
        }
        if (current.size() > 0) {
            return new Node<>(current, 0);
        }
        return null;
    }

    /**
     * Gets the last node of the subtree rooted at the given page.
     *
     * @param current   the root page of the subtree to look the last node for.
     * @param ancestors the list of ancestors for the current page.
     * @return the last node of the subtree rooted at the given page, or null if the page is null.
     * @throws IOException thrown by the Pager interface if any I/O errors occur.
     */
    Node<K, V> last(BTreePage<K, V> current, Deque<Node<K, V>> ancestors) throws IOException {
        while (!current.isLeaf()) {
            if (ancestors != null) {
                ancestors.addFirst(new Node<>(current, current.size()));
            }
            current = getChildPage(current, current.size());
        }
        if (current.size() > 0) {
            return new Node<>(current, current.size()-1);
        }
        return null;
    }

    /**
     * Returns the next node from the given current node.
     *
     * @param current   the starting node whose successor will be found.
     * @param ancestors the list of ancestors for the current node.
     * @return the node holding the key that follows the key of the current node,
     * or null if current node is the last node in the tree.
     * @throws IOException thrown by the Pager interface if any I/O errors occur.
     */
    Node<K, V> next(Node<K, V> current, Deque<Node<K, V>> ancestors) throws IOException {
        if (current.page.isLeaf()) {
            if (current.index + 1 < current.page.size()) {
                return new Node<>(current.page, current.index + 1);
            }
        } else {
            if (current.index < current.page.size()) {
                var parent = new Node<>(current.page, current.index + 1);
                ancestors.addFirst(parent);
                var child = getChildPage(parent.page, parent.index);
                return first(child, ancestors);
            }
        }

        while (!ancestors.isEmpty()) {
            var node = ancestors.removeFirst();
            if (node.index < node.page.size()) {
                return node;
            }
        }
        return null;
    }

    /**
     * Returns the previous node from the given current node.
     *
     * @param current   the starting node whose predecessor will be found.
     * @param ancestors the list of ancestors for the current node.
     * @return the node holding the key precedes the key of the current node,
     * or null if current node is the first node in the tree.
     * @throws IOException thrown by the Pager interface if any I/O errors occur.
     */
    Node<K, V> prev(Node<K, V> current, Deque<Node<K, V>> ancestors) throws IOException {
        if (current.page.isLeaf()) {
            if (current.index > 0) {
                return new Node<>(current.page, current.index - 1);
            }
        } else {
            ancestors.addFirst(current);
            var child = getChildPage(current.page, current.index);
            return last(child, ancestors);
        }

        while (!ancestors.isEmpty()) {
            var node = ancestors.removeFirst();
            if (node.index > 0) {
                return new Node<>(node.page, node.index - 1);
            }
        }
        return null;
    }

    /**
     * Recursively search for the specified key starting at the root of the tree, until either the key is found,
     * or a leaf node that does not contain the key is reached.
     *
     * @param key       the key to lookup.
     * @param ancestors the list of ancestors that will be populated as the structure is searched from top to bottom.
     * @return a result node referencing the given key if found, or the node from the last visited leaf page if the node does not exist.
     * @throws IOException thrown by the Pager interface if any I/O errors occur.
     */
    Result<K, V> lookup(K key, Deque<Node<K, V>> ancestors) throws IOException {
        var current = root;
        while (!current.isLeaf()) {
            var result = search(current, key);
            if (result.found) {
                return result;
            }

            if (ancestors != null) {
                ancestors.addFirst(result);
            }
            current = getChildPage(result.page, result.index);
        }

        return search(current, key);
    }

    /**
     * Performs a binary search for the specified key in the given page.
     *
     * @param page the page to search the given key.
     * @param key  the key to search for.
     * @return a result node containing either the node of the given key if found,
     * or the node with the smallest key greater than the given key.
     */
    Result<K, V> search(BTreePage<K, V> page, K key) {
        int p = 0;
        int r = page.size() - 1;

        while (p <= r) {
            int q = p + (r - p) / 2;

            var currentKey = page.key(q);
            if (Objects.equals(currentKey, key)) {
                return new Result<>(page, q, true);
            }

            if (key.compareTo(currentKey) <= 0) {
                r = q - 1;
            } else {
                p = q + 1;
            }
        }

        return new Result<>(page, p, false);
    }

    /**
     * Splits a page that has become full into two pages by creating a new page and moving half
     * of the nodes to the new page.
     *
     * @param source    the page to be split.
     * @param ancestors the ancestors of the current page.
     * @throws IOException thrown by the Pager interface if any I/O errors occur.
     */
    void split(BTreePage<K, V> source, Deque<Node<K, V>> ancestors) throws IOException {
        Node<K, V> parent;
        if (ancestors.isEmpty()) {
            parent = grow();
        } else {
            parent = ancestors.removeFirst();
        }

        int mid = source.size() / 2;
        var target = pager.create(source.isLeaf());
        move(source, target, mid + 1);
        promoteLast(source, parent);
        parent.page.child(parent.index, source.id());
        parent.page.child(parent.index + 1, target.id());

        if (parent.page.isFull()) {
            split(parent.page, ancestors);
        }
    }

    /**
     * Fills a page by borrowing from or merging it with either page to its side.
     *
     * @param target    the page to fill in.
     * @param ancestors the list of ancestors for the target page.
     * @throws IOException thrown by the Pager interface if any I/O errors occur.
     */
    void fill(BTreePage<K, V> target, Deque<Node<K, V>> ancestors) throws IOException {
        if (ancestors.isEmpty()) {
            shrink();
            return;
        }

        var parent = ancestors.removeFirst();
        if (borrowLeft(parent, target) || borrowRight(parent, target)) {
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
     * @return the first node of the new root page, which is the parent of the previous root.
     * @throws IOException thrown by the Pager interface if any I/O errors occur.
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
     * @throws IOException if any I/O error occurs while removing the root page or setting the new root page in the pager.
     */
    void shrink() throws IOException {
        if (root.size() == 0 && !root.isLeaf()) {
            pager.remove(root.id());
            root = getChildPage(root, 0);
            pager.root(root.id());
        }
    }

    /**
     * Tries to borrow the last element from the left page by making a right rotation to the target page.
     *
     * @param parent the parent node for the right rotation, must be the parent of the target page.
     * @param target the target page for the right rotation, the one that will receive the parent element.
     * @return true if the borrow was possible or false if the source page cannot be borrowed from or the
     * parent node is invalid.
     * @throws IOException if any I/O exception occurs reading the source page from the pager.
     */
    boolean borrowLeft(Node<K, V> parent, BTreePage<K, V> target) throws IOException {
        if (parent.index <= 0) {
            return false;
        }

        parent = new Node<>(parent.page, parent.index - 1);
        var source = getChildPage(parent.page, parent.index);
        if (!source.canBorrow()) {
            return false;
        }

        rotateRight(parent, source, target);
        return true;
    }

    /**
     * Tries to borrow the first element from the right page by making a left rotation to the target page.
     *
     * @param parent the parent node for the left rotation, must be the parent of the target page.
     * @param target the target page for the left rotation, the one that will receive the parent element.
     * @return true if the borrow was possible or false if the source page cannot be borrowed from or the parent node is invalid.
     * @throws IOException if any I/O exception occurs reading the source page from the pager.
     */
    boolean borrowRight(Node<K, V> parent, BTreePage<K, V> target) throws IOException {
        if (parent.index >= parent.page.size()) {
            return false;
        }

        var source = getChildPage(parent.page, parent.index + 1);
        if (!source.canBorrow()) {
            return false;
        }

        rotateLeft(parent, source, target);
        return true;
    }

    /**
     * Merges the target page with the page to the left
     *
     * @param parent the parent node of the given source page.
     * @param source the child page of the given parent which will be deleted
     *               after the merge and its elements moved to the target page.
     * @return true if the parent node could be merged, or false if the parent node has an invalid position.
     * @throws IOException if any I/O error occurs deleting the source page.
     */
    boolean mergeLeft(Node<K, V> parent, BTreePage<K, V> source) throws IOException {
        if (parent.index <= 0) {
            return false;
        }

        parent = new Node<>(parent.page, parent.index - 1);
        var target = getChildPage(parent.page, parent.index);
        merge(parent, source, target);
        return true;
    }

    /**
     * Merges the target page with the page to the right.
     *
     * @param parent the parent node of the given target page.
     * @param target the child page of the given parent node that will receive the elements from the source page.
     * @return true if the parent node could be merged, or false if the parent node has an invalid position.
     * @throws IOException if any I/O error occurs deleting the source page.
     */
    boolean mergeRight(Node<K, V> parent, BTreePage<K, V> target) throws IOException {
        if (parent.index >= parent.page.size()) {
            return false;
        }

        var source = getChildPage(parent.page, parent.index + 1);
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
        target.size(target.size() + 1);
        target.key(target.size() - 1, parent.page.key(parent.index));
        target.value(target.size() - 1, parent.page.value(parent.index));

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
     * @param index  a valid index at the source page signaling the first element that will be moved.
     */
    void move(BTreePage<K, V> source, BTreePage<K, V> target, int index) {
        int prevSize = target.size();
        int moveSize = source.size() - index;
        target.size(target.size() + moveSize);
        for (int i = 0; i <= moveSize; i++) {
            if (i < moveSize) {
                target.key(prevSize + i, source.key(index + i));
                target.value(prevSize + i, source.value(index + i));
            }
            if (!target.isLeaf()) {
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
        insertPlace(parent.page, parent.index);
        parent.page.key(parent.index, source.key(source.size() - 1));
        parent.page.value(parent.index, source.value(source.size() - 1));
        source.size(source.size() - 1);
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
        target.size(target.size() + 1);
        target.key(target.size() - 1, parent.page.key(parent.index));
        target.value(target.size() - 1, parent.page.value(parent.index));

        if (!target.isLeaf()) {
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
        insertPlace(target, 0);
        target.key(0, parent.page.key(parent.index));
        target.value(0, parent.page.value(parent.index));

        if (!target.isLeaf()) {
            target.child(0, source.child(source.size()));
        }

        parent.page.key(parent.index, source.key(source.size() - 1));
        parent.page.value(parent.index, source.value(source.size() - 1));
        source.size(source.size() - 1);
    }

    /**
     * This method inserts an empty spot at the given index by moving
     * every node one step to the right.
     *
     * @param page  The page to insert the place into.
     * @param index The index at which the empty spot will be inserted.
     */
    void insertPlace(BTreePage<K, V> page, int index) {
        page.size(page.size() + 1);
        for (int i = page.size(); i > index; i--) {
            if (i < page.size()) {
                page.key(i, page.key(i - 1));
                page.value(i, page.value(i - 1));
            }
            if (!page.isLeaf()) {
                page.child(i, page.child(i - 1));
            }
        }
    }

    /**
     * This method will delete the node at the given index by moving all
     * the remaining values one step left.
     *
     * @param page  The page to delete the node from.
     * @param index The index to be deleted from the given page.
     */
    void deletePlace(BTreePage<K, V> page, int index) {
        for (int i = index; i < page.size(); i++) {
            if (i < page.size() - 1) {
                page.key(i, page.key(i + 1));
                page.value(i, page.value(i + 1));
            }
            if (!page.isLeaf()) {
                page.child(i, page.child(i + 1));
            }
        }
        page.size(page.size() - 1);
    }

    /**
     * Gets the child page referenced by the specified node, this method will invoke the underlying pager.page()
     * method to load the child page.
     *
     * @param page  The non-leaf page to get the child id from.
     * @param index The valid index withing the bounds of page that holds the id of the child page.
     * @return The page object referenced by the parent page at the given index.
     * @throws IOException               thrown by the Pager interface if any I/O errors occur loading the page.
     * @throws IllegalArgumentException  if the page parameter is not a leaf page.
     * @throws IndexOutOfBoundsException if the page parameter is not a leaf page.
     */
    BTreePage<K, V> getChildPage(BTreePage<K, V> page, int index) throws IOException {
        return pager.page(page.child(index));
    }
}

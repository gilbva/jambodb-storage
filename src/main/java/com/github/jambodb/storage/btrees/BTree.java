package com.github.jambodb.storage.btrees;

import java.io.IOException;
import java.util.*;

public class BTree<K extends Comparable<K>, V> {
    private final Pager<BTreePage<K, V>> pager;
    private BTreePage<K, V> root;

    public BTree(Pager<BTreePage<K, V>> pager) throws IOException {
        this.pager = pager;
        this.root = pager.page(pager.root());
    }

    public V get(K key) throws IOException {
        var result = lookup(key, null);
        if (result.found) {
            return result.page.value(result.index);
        }
        return null;
    }

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

    public Iterator<BTreeEntry<K, V>> query(K from, K to) throws IOException {
        if (root.isLeaf() && root.size() == 0) {
            return Collections.emptyIterator();
        }

        final Deque<Node<K, V>> ancestors = new LinkedList<>();
        final Node<K, V> fromNode = from == null ? first(root, ancestors) : lookup(from, ancestors);

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
                        if (key.compareTo(to) > 0) {
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

    private Node<K, V> first(BTreePage<K, V> current, Deque<Node<K, V>> ancestors) throws IOException {
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

    private Node<K, V> last(BTreePage<K, V> current, Deque<Node<K, V>> ancestors) throws IOException {
        while (!current.isLeaf()) {
            if (ancestors != null) {
                ancestors.addFirst(new Node<>(current, 0));
            }
            current = getChildPage(current, current.size());
        }
        if (current.size() > 0) {
            return new Node<>(current, current.size());
        }
        return null;
    }

    private Node<K, V> next(Node<K, V> current, Deque<Node<K, V>> ancestors) throws IOException {
        if (current.page.isLeaf()) {
            if (current.index + 1 < current.page.size()) {
                return new Node<>(current.page, current.index + 1);
            }
        } else {
            if (current.index < current.page.size()) {
                ancestors.addFirst(new Node<>(current.page, current.index + 1));
                return first(getChildPage(current.page, current.index + 1), ancestors);
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

    private Node<K, V> prev(Node<K, V> current, Deque<Node<K, V>> ancestors) throws IOException {
        if (current.page.isLeaf()) {
            if (current.index - 1 >= 0) {
                return new Node<>(current.page, current.index - 1);
            }
        } else {
            if (current.index >= 0) {
                ancestors.addFirst(current);
                return last(getChildPage(current.page, current.index), ancestors);
            }
        }

        while (!ancestors.isEmpty()) {
            var node = ancestors.removeFirst();
            if (node.index - 1 >= 0) {
                return new Node<>(node.page, node.index - 1);
            }
        }
        return null;
    }

    private Result<K, V> lookup(K key, Deque<Node<K, V>> ancestors) throws IOException {
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

    private Result<K, V> search(BTreePage<K, V> page, K key) {
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

    private void split(BTreePage<K, V> source, Deque<Node<K, V>> ancestors) throws IOException {
        Node<K, V> parent;
        if (ancestors.isEmpty()) {
            parent = grow();
        } else {
            parent = ancestors.removeFirst();
        }

        int mid = source.size() / 2;
        var target = pager.create(source.isLeaf());
        move(source, mid + 1, target);
        promoteLast(source, parent);
        parent.page.child(parent.index, source.id());
        parent.page.child(parent.index + 1, target.id());

        if (parent.page.isFull()) {
            split(parent.page, ancestors);
        }
    }

    private void fill(BTreePage<K, V> target, Deque<Node<K, V>> ancestors) throws IOException {
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

    private Node<K, V> grow() throws IOException {
        var newRoot = pager.create(false);
        newRoot.child(0, root.id());
        root = newRoot;
        pager.root(root.id());
        return new Node<>(root, 0);
    }

    private void shrink() throws IOException {
        if (root.size() == 0 && !root.isLeaf()) {
            pager.remove(root.id());
            root = getChildPage(root, 0);
            pager.root(root.id());
        }
    }

    private boolean borrowLeft(Node<K, V> parent, BTreePage<K, V> target) throws IOException {
        if (parent.index <= 0) {
            return false;
        }

        var source = getChildPage(parent.page, parent.index);
        if (source.canNotBorrow()) {
            return false;
        }

        rotateRight(parent, source, target);
        return true;
    }

    private boolean borrowRight(Node<K, V> parent, BTreePage<K, V> target) throws IOException {
        if (parent.index >= parent.page.size()) {
            return false;
        }

        var source = getChildPage(parent.page, parent.index + 1);
        if (source.canNotBorrow()) {
            return false;
        }

        rotateLeft(parent, source, target);
        return true;
    }

    private boolean mergeLeft(Node<K, V> parent, BTreePage<K, V> source) throws IOException {
        if (parent.index <= 0) {
            return false;
        }

        parent.index--;
        var target = getChildPage(parent.page, parent.index);
        merge(parent, source, target);
        return true;
    }

    private boolean mergeRight(Node<K, V> parent, BTreePage<K, V> target) throws IOException {
        if (parent.index >= parent.page.size()) {
            return false;
        }

        var source = getChildPage(parent.page, parent.index + 1);
        merge(parent, source, target);
        return true;
    }

    private void merge(Node<K, V> parent, BTreePage<K, V> source, BTreePage<K, V> target) throws IOException {
        target.size(target.size() + 1);
        target.key(target.size() - 1, parent.page.key(parent.index));
        target.value(target.size() - 1, parent.page.value(parent.index));

        deletePlace(parent.page, parent.index);
        parent.page.child(parent.index, target.id());

        move(source, 0, target);
        pager.remove(source.id());
    }

    private void move(BTreePage<K, V> source, int index, BTreePage<K, V> target) {
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

    private void promoteLast(BTreePage<K, V> source, Node<K, V> parent) {
        insertPlace(parent.page, parent.index);
        parent.page.key(parent.index, source.key(source.size() - 1));
        parent.page.value(parent.index, source.value(source.size() - 1));
        source.size(source.size() - 1);
    }

    private void rotateLeft(Node<K, V> parent, BTreePage<K, V> source, BTreePage<K, V> target) {
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

    private void rotateRight(Node<K, V> parent, BTreePage<K, V> source, BTreePage<K, V> target) {
        insertPlace(target, 0);
        target.key(0, parent.page.key(parent.index));
        target.value(0, parent.page.value(parent.index));
        if (!target.isLeaf()) {
            target.child(0, source.child(source.size()));
        }
        parent.page.key(parent.index, source.key(source.size() - 1));
        source.size(source.size() - 1);
    }

    private void insertPlace(BTreePage<K, V> page, int index) {
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

    private void deletePlace(BTreePage<K, V> page, int index) {
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

    private BTreePage<K, V> getChildPage(BTreePage<K, V> page, int index) throws IOException {
        return pager.page(page.child(index));
    }

    private static class Node<K, V> implements BTreeEntry<K, V> {
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
    }

    private static class Result<K, V> extends Node<K, V> {
        boolean found;

        public Result(BTreePage<K, V> page, int index, boolean found) {
            super(page, index);
            this.found = found;
        }
    }
}

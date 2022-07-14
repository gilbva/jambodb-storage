package me.gilbva.jambodb.storage.pager;

import java.util.HashMap;
import java.util.Map;

public class LRUPagesCache<K, V> {
    private static class CacheEntry {
        CacheEntry next;
        CacheEntry prev;
        int id;

        public CacheEntry(int id) {
            this.id = id;
        }
    }

    private final Map<Integer, SlottedBTreePage<K, V>> pages = new HashMap<>();

    private final Map<Integer, CacheEntry> entries = new HashMap<>();

    private CacheEntry first;

    private CacheEntry last;

    private final int maxSize;

    public LRUPagesCache(int maxSize) {
        this.maxSize = maxSize;
    }

    public boolean contains(int id) {
        return entries.containsKey(id);
    }

    public SlottedBTreePage<K, V> get(int id) {
        if(entries.containsKey(id)) {
            if(first.id != id) {
                addEntry(removeEntry(entries.get(id)));
            }
        }

        return pages.get(id);
    }

    public void put(SlottedBTreePage<K, V> page) {
        if(entries.containsKey(page.id())) {
            if(first.id != page.id()) {
                addEntry(removeEntry(entries.get(page.id())));
            }
        }
        else {
            addEntry(new CacheEntry(page.id()));
        }

        pages.put(page.id(), page);
        if(pages.size() > maxSize) {
            evit();
        }
    }

    public void remove(SlottedBTreePage<K, V> page) {
        if(entries.containsKey(page.id())) {
            removeEntry(entries.get(page.id()));
            pages.remove(page.id());
        }
    }

    public Iterable<SlottedBTreePage<K, V>> all() {
        return pages.values();
    }

    public void evit() {
        var current = last;
        while(current != null && pages.size() > maxSize) {
            if(!pages.get(current.id).isModified()) {
                removeEntry(current);
                pages.remove(current.id);
            }

            current = current.prev;
        }
    }

    public void evitAll() {
        pages.clear();
        entries.clear();
        first = null;
        last = null;
    }


    private void addEntry(CacheEntry entry) {
        entries.put(entry.id, entry);

        if(first == null) {
            first = entry;
            last = entry;
        }
        else {
            entry.next = first;
            first.prev = entry;
            first = entry;
        }
    }

    private CacheEntry removeEntry(CacheEntry entry) {
        entries.remove(entry.id);

        if(entry.prev == null) {
            first = entry.next;
            if(first != null) {
                first.prev = null;
            }
        }
        else {
            entry.prev.next = entry.next;
        }

        if(entry.next == null) {
            last = entry.prev;
            if(last != null) {
                last.next = null;
            }
        }
        else {
            entry.next.prev = entry.prev;
        }

        entry.prev = null;
        entry.next = null;
        return entry;
    }
}

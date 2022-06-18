package me.gilbva.jambodb.storage.pager;

import me.gilbva.jambodb.storage.blocks.SecurityOptions;
import me.gilbva.jambodb.storage.btrees.Serializer;

import java.io.IOException;
import java.nio.file.Path;

class FilePagerOptions<K, V> implements FilePagerBuilder<K, V> {
    private Path file;

    private int cachePages;

    private boolean init;

    private Serializer<K> keySer;

    private Serializer<V> valueSer;

    private SecurityOptions security;

    public FilePagerOptions(boolean init,
                            Serializer<K> keySer,
                            Serializer<V> valueSer) {
        this.init = init;
        this.keySer = keySer;
        this.valueSer = valueSer;
    }

    public Path file() {
        return file;
    }

    public int cachePages() {
        return cachePages;
    }

    public boolean init() {
        return init;
    }

    public Serializer<K> keySerializer() {
        return keySer;
    }

    public Serializer<V> valueSerializer() {
        return valueSer;
    }

    public SecurityOptions security() {
        return security;
    }

    @Override
    public FilePagerBuilder<K, V> file(Path file) {
        this.file = file;
        return this;
    }

    @Override
    public FilePagerBuilder<K, V> cachePages(int value) {
        this.cachePages = value;
        return this;
    }

    @Override
    public FilePagerBuilder<K, V> security(SecurityOptions opts) {
        this.security = opts;
        return this;
    }

    @Override
    public FilePager<K, V> build() throws IOException {
        return new FilePager<K, V>(this);
    }
}

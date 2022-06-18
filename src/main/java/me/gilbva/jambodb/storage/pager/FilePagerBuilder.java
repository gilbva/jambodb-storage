package me.gilbva.jambodb.storage.pager;

import me.gilbva.jambodb.storage.blocks.SecurityOptions;

import java.io.IOException;
import java.nio.file.Path;

public interface FilePagerBuilder<K, V> {
    FilePagerBuilder<K, V> file(Path file);

    FilePagerBuilder<K, V> cachePages(int value);

    FilePagerBuilder<K, V> security(SecurityOptions opts);

    FilePager<K, V> build() throws IOException;
}

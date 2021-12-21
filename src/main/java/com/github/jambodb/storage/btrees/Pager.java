package com.github.jambodb.storage.btrees;

import java.io.IOException;

public interface Pager<P> {
    int root() throws IOException;

    void root(int id) throws IOException;

    P page(int id) throws IOException;

    P create(boolean leaf) throws IOException;

    void remove(int id) throws IOException;

    void fsync() throws IOException;
}

package me.gilbva.jambodb.storage.btrees;

import java.io.IOException;

/**
 * This interface represents a repository of pages, this is the mechanism used
 * by the Btree class to communicate with external memory.
 *
 * @param <P> The actual type of the page managed by this repository.
 */
public interface Pager<P> {
    /**
     * Gets the current root page saved in this repository.
     *
     * @return the numeric id of the root page.
     * @throws IOException if any I/O exception occurs accessing the underlying external memory.
     */
    int root() throws IOException;

    /**
     * Sets the id of root page
     *
     * @param id the numeric id of the root page.
     * @throws IOException if any I/O exception occurs accessing the underlying external memory.
     */
    void root(int id) throws IOException;

    /**
     * Gets the page by the given id. this method will load the page from disk if necessary.
     *
     * @param id the id of the page to get.
     * @return the actual page loaded from disk or null if the page does not exist.
     * @throws IOException if any I/O exception occurs accessing the underlying external memory.
     */
    P page(int id) throws IOException;

    /**
     * Creates a new page in this repository.
     *
     * @param leaf if the page is a leaf page.
     * @return the new created page.
     * @throws IOException if any I/O exception occurs accessing the underlying external memory.
     */
    P create(boolean leaf) throws IOException;

    /**
     * Removes the given page from this store.
     *
     * @param id the id of the page to be removed.
     * @throws IOException if any I/O exception occurs accessing the underlying external memory.
     */
    void remove(int id) throws IOException;

    /**
     * Synchronizes the changes made to this repository to disk.
     *
     * @throws IOException if any I/O exception occurs accessing the underlying external memory.
     */
    void fsync() throws IOException;
}

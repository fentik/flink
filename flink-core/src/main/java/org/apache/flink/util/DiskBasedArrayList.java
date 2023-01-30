package org.apache.flink.util;

import java.io.*;
import java.util.*;
import java.util.function.Consumer;

/**
 * Disk-backed array list
 * @param <E> element type
 */
public class DiskBasedArrayList<E> extends ArrayList<E> implements AutoCloseable {
    private int totalSize;
    private int threshold;
    private File backingFile;
    private ObjectOutputStream out;
    private List<E> prependedEntries = new ArrayList<>();

    public DiskBasedArrayList(int threshold) {
        this.threshold = threshold;
        try {
            this.backingFile = File.createTempFile(UUID.randomUUID().toString(), ".list");
            this.out = new ObjectOutputStream(new FileOutputStream(backingFile, true));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public int size() {
        return totalSize;
    }

    @Override
    public boolean add(E e) {
        boolean result = super.add(e);
        totalSize++;
        if (super.size() > threshold) {
            flushToDisk();
            super.clear();
        }
        return result;
    }

    @Override
    public void add(int index, E element) {
        if (index == 0) {
            prependedEntries.add(element);
            totalSize++;
        } else {
            throw new UnsupportedOperationException("Adding element at random index not supported");
        }
    }

    private void flushToDisk() {
        try {
            // using the super iterator which is memory-only
            for (Iterator<E> it = super.iterator(); it.hasNext();) {
                out.writeObject(it.next());
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Iterator<E> iterator() {
        if (totalSize < threshold) {
            return super.iterator();
        } else {
            return new DiskBasedIterator(super.iterator());
        }
    }

    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        if (totalSize < threshold) {
            return super.subList(fromIndex, toIndex);
        } else {
            throw new UnsupportedOperationException("Sublist not supported");
        }
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        throw new UnsupportedOperationException("addAll not supported");
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        throw new UnsupportedOperationException("addAll not supported");
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("Removing is not supported");
    }

    @Override
    public E remove(int index) {
        throw new UnsupportedOperationException("Removing is not supported");
    }

    @Override
    public void clear() {
        super.clear();
        prependedEntries.clear();
        try {
            out.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        backingFile.delete();
    }

    @Override
    public void close() throws Exception {
        clear();
    }

    /**
     * Iterator that fetches records from disk
     */
    private class DiskBasedIterator implements Iterator<E> {
        private int cursor;       // index of next element to return
        private int expectedModCount = modCount;
        private ObjectInputStream in;
        private Iterator<E> memoryIterator;
        private Iterator<E> prependedIterator;
        // prevent creating a synthetic constructor
        DiskBasedIterator(Iterator<E> memoryIterator) {
            try {
                in = new ObjectInputStream(new FileInputStream(backingFile));
                this.memoryIterator = memoryIterator;
                this.prependedIterator = prependedEntries.iterator();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        public boolean hasNext() {
            return cursor != totalSize;
        }

        @SuppressWarnings("unchecked")
        public E next() {
            checkForComodification();
            int i = cursor;
            if (i >= totalSize) {
                throw new NoSuchElementException();
            }
            cursor = i + 1;
            try {
                if (prependedIterator.hasNext()) {
                    return prependedIterator.next();
                } else if (in != null) {
                    try {
                        return (E) in.readObject();
                    } catch (EOFException ex) {
                        in.close();
                        in = null;
                        return memoryIterator.next();
                    }
                } else {
                    return memoryIterator.next();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void remove() {
            throw new UnsupportedOperationException("Removing from disk backed array is not allowed");
        }

        @Override
        public void forEachRemaining(Consumer<? super E> action) {
            throw new UnsupportedOperationException("forEachRemaining not supported");
        }

        final void checkForComodification() {
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
        }
    }
}

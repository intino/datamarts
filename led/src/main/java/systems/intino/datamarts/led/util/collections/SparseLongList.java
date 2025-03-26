package systems.intino.datamarts.led.util.collections;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class SparseLongList implements LongList {

    private static final int DEFAULT_INITIAL_CAPACITY = 10;
    private static final int DEFAULT_ARRAY_SIZE = 1024;
    private static final float DEFAULT_GROW_FACTOR = 2.0f;

    private long[][] arrays;
    private final int arraySize;
    private int arrayIndex;
    private int relativeIndex;
    private float growFactor;

    public SparseLongList() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_ARRAY_SIZE, DEFAULT_GROW_FACTOR);
    }

    public SparseLongList(int initialCapacity, int arraySize, float growFactor) {
        arrays = new long[initialCapacity][];
        arrays[0] = new long[arraySize];
        this.arraySize = arraySize;
        growFactor(growFactor);
    }

    @Override
    public int size() {
        return arrayIndex * arraySize + relativeIndex;
    }

    @Override
    public int capacity() {
        return arrays.length * arraySize;
    }

    @Override
    public float growFactor() {
        return growFactor;
    }

    @Override
    public void growFactor(float growFactor) {
        if(growFactor <= 1) {
            throw new IllegalArgumentException("Grow factor must be > 0");
        }
        this.growFactor = growFactor;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(long o) {
        return parallelStream().anyMatch(e -> e == o);
    }

    @Override
    public Iterator<Long> iterator() {
        return new SparseLongListIterator();
    }

    @Override
    public void add(long element) {
        if(relativeIndex >= arraySize) {
            if(++arrayIndex >= arrays.length) {
                grow();
            }
            arrays[arrayIndex] = new long[arraySize];
            relativeIndex = 0;
        }
        arrays[arrayIndex][relativeIndex++] = element;
    }

    private void grow() {
        final long newCapacity = (long) Math.ceil(capacity() * growFactor);
        if(newCapacity > Integer.MAX_VALUE) {
            throw new OutOfMemoryError();
        }
        grow((int) newCapacity);
    }

    private void grow(int newCapacity) {
        arrays = Arrays.copyOf(arrays, newCapacity);
    }

    @Override
    public boolean containsAll(Iterable<Long> c) {
        int i = 0;
        for(Long element : c) {
            if(element == null) {
                return false;
            }
            if(element != get(i++)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void addAll(Iterable<Long> c) {
        c.forEach(this::add);
    }

    @Override
    public void clear() {
        arrayIndex = relativeIndex = 0;
    }

    @Override
    public long get(int index) {
        final int arrayIndex = index / arraySize;
        final int relativeIndex = index % arraySize;
        return arrays[arrayIndex][relativeIndex];
    }

    public long get(int arrayIndex, int relativeIndex) {
        return arrays[arrayIndex][relativeIndex];
    }

    @Override
    public long set(int index, long element) {
        final int arrayIndex = index / arraySize;
        final int relativeIndex = index % arraySize;
        final long oldValue = arrays[arrayIndex][relativeIndex];
        arrays[arrayIndex][relativeIndex] = element;
        return oldValue;
    }

    @Override
    public LongStream stream() {
        return IntStream.range(0, size()).mapToLong(this::get);
    }

    @Override
    public LongStream parallelStream() {
        return stream().parallel();
    }

    @Override
    public List<Long> asList() {
        return new AbstractList<>() {
            @Override
            public Long get(int index) {
                return SparseLongList.this.get(index);
            }

            @Override
            public int size() {
                return SparseLongList.this.size();
            }
        };
    }

    private class SparseLongListIterator implements Iterator<Long> {

        private int iteratorArrayIndex;
        private int iteratorRelativeIndex;

        @Override
        public boolean hasNext() {
            return iteratorArrayIndex * arraySize + iteratorRelativeIndex < size();
        }

        @Override
        public Long next() {
            if(iteratorRelativeIndex >= arraySize) {
                ++iteratorArrayIndex;
                iteratorRelativeIndex = 0;
            }
            return get(iteratorArrayIndex, iteratorRelativeIndex++);
        }
    }
}

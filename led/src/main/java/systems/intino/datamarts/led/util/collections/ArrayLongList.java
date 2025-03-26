package systems.intino.datamarts.led.util.collections;

import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class ArrayLongList implements LongList {

    private static final int DEFAULT_INITIAL_CAPACITY = 10;
    private static final int DEFAULT_GROW_FACTOR = 2;

    private long[] data;
    private int size;
    private float growFactor;

    public ArrayLongList() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_GROW_FACTOR);
    }

    public ArrayLongList(int initialCapacity, float growFactor) {
        data = new long[initialCapacity];
        growFactor(growFactor);
    }

    public ArrayLongList(long[] data) {
        this(data.length, DEFAULT_GROW_FACTOR);
        this.data = data;
        this.size = data.length;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int capacity() {
        return data.length;
    }

    public long[] data() {
        return data;
    }

    public void trimToSize() {
        if(size < data.length) {
            data = Arrays.copyOf(data, size);
        }
    }

    @Override
    public float growFactor() {
        return growFactor;
    }

    @Override
    public void growFactor(float growFactor) {
        if(growFactor <= 0) {
            throw new IllegalArgumentException("Grow factor must be > 0");
        }
        this.growFactor = growFactor;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean contains(long o) {
        return parallelStream().anyMatch(e -> e == o);
    }

    @Override
    public Iterator<Long> iterator() {
        return listIterator();
    }

    @Override
    public void add(long element) {
        if(size >= capacity()) {
            grow();
        }
        data[size++] = element;
    }

    private void grow() {
        final long newCapacity = (long) Math.ceil(capacity() * growFactor);
        if(newCapacity > Integer.MAX_VALUE) {
            throw new OutOfMemoryError();
        }
        grow((int) newCapacity);
    }

    private void grow(int newCapacity) {
        data = Arrays.copyOf(data, newCapacity);
    }

    public boolean remove(long value) {
        final long[] data = this.data;
        final int size = this.size;
        int i = 0;
        for(;i < size;i++) {
            if(data[i] == value) {
                break;
            }
        }
        if(i == size) {
            return false;
        }
        fastRemove(data, i);
        return true;
    }

    private void fastRemove(long[] data, int indexToRemove) {
        if(--size > indexToRemove) {
            System.arraycopy(data, indexToRemove + 1, data, indexToRemove, size - indexToRemove);
        }
    }

    @Override
    public boolean containsAll(Iterable<Long> c) {
        int i = 0;
        for(Long element : c) {
            if(element == null) {
                return false;
            }
            if(element != data[i++]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void addAll(Iterable<Long> c) {
        if(c instanceof ArrayLongList) {
            ArrayLongList other = (ArrayLongList) c;
            if(size + other.size > capacity()) {
                grow(size + other.size);
            }
            System.arraycopy(other.data, 0, data, size, other.size);
            size += other.size;
        } else {
            c.forEach(this::add);
        }
    }

    @Override
    public void clear() {
        size = 0;
    }

    @Override
    public long get(int index) {
        return data[index];
    }

    @Override
    public long set(int index, long element) {
        final long oldValue = data[index];
        data[index] = element;
        return oldValue;
    }

    public void add(int index, long element) {
        if(size >= capacity()) {
            grow();
        }
        System.arraycopy(data, index, data, index + 1, size - index);
        data[index] = element;
        ++size;
    }

    public long remove(int index) {
        final long oldValue = data[index];
        fastRemove(data, index);
        return oldValue;
    }

    public int indexOf(long valueToFind) {
        for(int i = 0;i < size;i++) {
            if(data[i] == valueToFind) {
                return i;
            }
        }
        return -1;
    }

    public int lastIndexOf(long valueToFind) {
        for(int i = size - 1;i >= 0;i--) {
            if(data[i] == valueToFind) {
                return i;
            }
        }
        return -1;
    }

    public int binarySearch(long value) {
        return 0;
    }

    public ListIterator<Long> listIterator() {
        return new JvmGrowableLongArrayListIterator(0);
    }

    public ListIterator<Long> listIterator(int index) {
        return new JvmGrowableLongArrayListIterator(index);
    }

    @Override
    public LongStream stream() {
        return IntStream.range(0, size).mapToLong(this::get);
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
                return ArrayLongList.this.get(index);
            }

            @Override
            public int size() {
                return size;
            }
        };
    }

    public final class JvmGrowableLongArrayListIterator implements ListIterator<Long> {

        private int index;

        public JvmGrowableLongArrayListIterator(int index) {
            this.index = index;
        }

        @Override
        public boolean hasNext() {
            return index < size;
        }

        @Override
        public Long next() {
            return data[index++];
        }

        @Override
        public boolean hasPrevious() {
            return index - 1 >= 0;
        }

        @Override
        public Long previous() {
            return data[index--];
        }

        @Override
        public int nextIndex() {
            return index + 1;
        }

        @Override
        public int previousIndex() {
            return index - 1;
        }

        @Override
        public void remove() {
            ArrayLongList.this.remove(index);
            index = Math.max(0, index - 1);
        }

        @Override
        public void set(Long aLong) {
            if(aLong == null) {
                throw new NullPointerException();
            }
            data[index] = aLong;
        }

        @Override
        public void add(Long aLong) {
            ArrayLongList.this.add(index, aLong);
        }
    }
}

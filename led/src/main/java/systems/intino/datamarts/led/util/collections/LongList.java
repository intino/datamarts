package systems.intino.datamarts.led.util.collections;

import java.util.List;
import java.util.stream.LongStream;

public interface LongList extends Iterable<Long> {

    int capacity();
    int size();
    boolean isEmpty();
    float growFactor();
    void growFactor(float growFactor);
    void add(long value);
    long set(int index, long value);
    long get(int index);
    boolean contains(long value);
    void clear();
    void addAll(Iterable<Long> other);
    boolean containsAll(Iterable<Long> other);
    LongStream stream();
    LongStream parallelStream();
    List<Long> asList();
}

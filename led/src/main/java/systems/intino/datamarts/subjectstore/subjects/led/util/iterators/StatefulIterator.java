package systems.intino.datamarts.subjectstore.subjects.led.util.iterators;

import java.util.Iterator;

public interface StatefulIterator<T> extends Iterator<T> {

    static <T> StatefulIterator<T> of(Iterator<T> iterator) {
        return iterator instanceof StatefulIterator ? (StatefulIterator<T>) iterator : new SimpleStatefulIterator<>(iterator);
    }

    T current();
}

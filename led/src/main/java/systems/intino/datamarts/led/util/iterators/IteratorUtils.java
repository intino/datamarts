package systems.intino.datamarts.led.util.iterators;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class IteratorUtils {

    public static <T> Stream<T> streamOf(Iterator<T> iterator) {
        return StreamSupport.stream(spliteratorOf(iterator), false);
    }

    public static <T> Spliterator<T> spliteratorOf(Iterator<T> iterator) {
        return Spliterators.spliteratorUnknownSize(iterator, Spliterator.SORTED);
    }

}

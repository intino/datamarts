package systems.intino.datamarts.led.leds;

import systems.intino.datamarts.led.LedStream;
import systems.intino.datamarts.led.Schema;

import java.util.Iterator;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class IteratorLedStream<T extends Schema> implements LedStream<T> {

    public static <S extends Schema> IteratorLedStream<S> fromStream(Class<S> schemaClass, Stream<S> stream) {
        return fromStream(schemaClass, Schema.sizeOf(schemaClass), stream);
    }

    public static <S extends Schema> IteratorLedStream<S> fromStream(Class<S> schemaClass, int schemaSize, Stream<S> stream) {
        return new IteratorLedStream<>(schemaClass, schemaSize, stream.iterator());
    }


    private final Class<T> schemaClass;
    private final Iterator<T> iterator;
    private final int schemaSize;
    private Runnable onClose;

    public IteratorLedStream(Class<T> schemaClass, Iterator<T> iterator) {
        this(schemaClass, Schema.sizeOf(schemaClass), iterator);
    }

    public IteratorLedStream(Class<T> schemaClass, int schemaSize, Iterator<T> iterator) {
        this.schemaClass = schemaClass;
        this.iterator = requireNonNull(iterator);
        this.schemaSize = schemaSize;
    }

    @Override
    public void close() throws Exception {
        if(onClose != null) {
            onClose.run();
            onClose = null;
        }
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        return iterator.next();
    }

    @Override
    public LedStream<T> onClose(Runnable onClose) {
        this.onClose = onClose;
        return this;
    }

    @Override
    public Class<T> schemaClass() {
        return schemaClass;
    }

    @Override
    public int schemaSize() {
        return schemaSize;
    }
}

package systems.intino.datamarts.led.leds;

import systems.intino.datamarts.led.Led;
import systems.intino.datamarts.led.Schema;

public class ArrayLed<T extends Schema> implements Led<T> {

    public static <S extends Schema> ArrayLed<S> fromIndexedLed(IndexedLed<S> led, S[] array) {
        return new ArrayLed<>(led.schemaClass(), led.asList().toArray(array));
    }


    private final Class<T> schemaClass;
    private final T[] schemas;
    private final int schemaSize;
    private final int length;

    public ArrayLed(Class<T> schemaClass, T[] schemas, int length) {
        this.schemaClass = schemaClass;
        this.schemas = schemas;
        this.schemaSize = Schema.sizeOf(schemaClass);
        this.length = length;
    }

    public ArrayLed(Class<T> schemaClass, T[] schemas) {
        this(schemaClass, schemas, schemas.length);
    }

    @Override
    public long size() {
        return length;
    }

    @Override
    public int schemaSize() {
        return schemaSize;
    }

    @Override
    public T schema(int index) {
        return schemas[index];
    }

    @Override
    public Class<T> schemaClass() {
        return schemaClass;
    }

    public T[] array() {
        return schemas;
    }
}

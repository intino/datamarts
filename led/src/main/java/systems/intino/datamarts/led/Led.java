package systems.intino.datamarts.led;

import systems.intino.datamarts.led.allocators.indexed.IndexedAllocator;
import systems.intino.datamarts.led.leds.IteratorLedStream;
import systems.intino.datamarts.led.leds.ListLed;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public interface Led<T extends Schema> extends Iterable<T> {

	static <T extends Schema> Led<T> empty(Class<T> schemaClass) {
		return new ListLed<>(schemaClass, Collections.emptyList());
	}

	static <T extends Schema> Led<T> fromLedStream(LedStream<T> ledStream) {
		return new ListLed<>(ledStream.schemaClass(), ledStream.asJavaStream().collect(Collectors.toUnmodifiableList()));
	}

	static <T extends Schema> Builder<T> builder(Class<T> schemaClass) {
		return new LedBuilder<>(schemaClass);
	}

	static <T extends Schema> Builder<T> builder(Class<T> schemaClass, IndexedAllocator<T> allocator) {
		return new LedBuilder<>(schemaClass, allocator);
	}

	long size();

	int schemaSize();

	T schema(int index);

	Class<T> schemaClass();

	@Override
	default Iterator<T> iterator() {
		return elements().iterator();
	}

	default List<T> elements() {
		return new AbstractList<T>() {
			@Override
			public T get(int index) {
				return schema(index);
			}

			@Override
			public int size() {
				return (int) Led.this.size();
			}
		};
	}

	default LedStream<T> toLedStream() {
		return new IteratorLedStream<>(schemaClass(), iterator());
	}

    default UUID serialUUID() {
		return Schema.getSerialUUID(schemaClass());
	}

    interface Builder<T extends Schema> {

		Class<T> schemaClass();

		int schemaSize();

		Builder<T> create(Consumer<T> initializer);

		Led<T> build();
	}
}
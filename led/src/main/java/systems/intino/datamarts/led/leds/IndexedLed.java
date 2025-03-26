package systems.intino.datamarts.led.leds;

import systems.intino.datamarts.led.Led;
import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.indexed.IndexedAllocator;

import java.util.AbstractList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class IndexedLed<T extends Schema> implements Led<T> {

	private final IndexedAllocator<T> allocator;

	public IndexedLed(IndexedAllocator<T> allocator) {
		this.allocator = requireNonNull(allocator);
	}

	public long size() {
		return allocator.size();
	}

	@Override
	public int schemaSize() {
		return allocator.schemaSize();
	}

	@Override
	public T schema(int index) {
		if(index >= size()) throw new IndexOutOfBoundsException("Index >= " + size());
		return allocator.malloc(index);
	}

	@Override
	public Class<T> schemaClass() {
		return allocator.schemaClass();
	}

	public List<T> asList() {
		return new AbstractList<T>() {
			@Override
			public T get(int index) {
				return schema(index);
			}

			@Override
			public int size() {
				return (int) IndexedLed.this.size();
			}
		};
	}
}

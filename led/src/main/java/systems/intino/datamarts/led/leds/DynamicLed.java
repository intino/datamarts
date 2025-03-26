package systems.intino.datamarts.led.leds;

import systems.intino.datamarts.led.Led;
import systems.intino.datamarts.led.LedLibraryConfig;
import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.indexed.ListAllocator;

import java.util.AbstractList;
import java.util.List;

public class DynamicLed<T extends Schema> implements Led<T> {

	private final ListAllocator<T> allocator;
	private final int schemaSize;

	public DynamicLed(Class<T> schemaClass) {
		this.schemaSize = Schema.sizeOf(schemaClass);
		this.allocator = new ListAllocator<>(LedLibraryConfig.DEFAULT_BUFFER_SIZE.get(), schemaSize, schemaClass);
	}

	public Schema newTransaction() {
		return allocator.malloc();
	}

	@Override
	public long size() {
		return allocator.size();
	}

	@Override
	public int schemaSize() {
		return schemaSize;
	}

	@Override
	public T schema(int index) {
		if(index >= size()) {
			throw new IndexOutOfBoundsException("Index >= " + size());
		}
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
				return (int) DynamicLed.this.size();
			}
		};
	}
}

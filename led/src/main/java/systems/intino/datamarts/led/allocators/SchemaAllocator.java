package systems.intino.datamarts.led.allocators;

import systems.intino.datamarts.led.Schema;

public interface SchemaAllocator<T extends Schema> extends AutoCloseable {

	long size();
	T malloc();
	T calloc();
	int schemaSize();
	void clear();
	void free();
	Class<T> schemaClass();

	@Override
	default void close() {
		free();
	}
}

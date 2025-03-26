package systems.intino.datamarts.led.allocators.stack;

import systems.intino.datamarts.led.Schema;

@FunctionalInterface
public interface StackAllocatorFactory<T extends Schema> {

	StackAllocator<T> create(int elementSize, long elementCount, Class<T> schemaClass);
}

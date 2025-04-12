package systems.intino.dataformats.led.allocators.stack;

import systems.intino.dataformats.led.Schema;

@FunctionalInterface
public interface StackAllocatorFactory<T extends Schema> {

	StackAllocator<T> create(int elementSize, long elementCount, Class<T> schemaClass);
}

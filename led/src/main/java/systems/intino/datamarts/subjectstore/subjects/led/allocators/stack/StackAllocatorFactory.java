package systems.intino.datamarts.subjectstore.subjects.led.allocators.stack;

import systems.intino.datamarts.subjectstore.subjects.led.Schema;

@FunctionalInterface
public interface StackAllocatorFactory<T extends Schema> {

	StackAllocator<T> create(int elementSize, long elementCount, Class<T> schemaClass);
}

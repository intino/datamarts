package systems.intino.datamarts.subjectstore.subjects.led.allocators.stack;

import systems.intino.datamarts.subjectstore.subjects.led.Schema;
import systems.intino.datamarts.subjectstore.subjects.led.allocators.SchemaAllocator;

public interface StackAllocator<T extends Schema> extends SchemaAllocator<T> {

	long stackPointer();

	long address();

	long stackSize();

	long remainingBytes();

	T malloc();

	T calloc();

	void pop();

	void clear();
}

package systems.intino.datamarts.led.allocators.stack;

import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.SchemaAllocator;

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

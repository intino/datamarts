package systems.intino.dataformats.led.allocators.stack;

import systems.intino.dataformats.led.Schema;
import systems.intino.dataformats.led.allocators.SchemaAllocator;

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

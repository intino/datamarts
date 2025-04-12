package systems.intino.dataformats.led.allocators.indexed;

import systems.intino.dataformats.led.Schema;
import systems.intino.dataformats.led.allocators.SchemaAllocator;

public interface IndexedAllocator<T extends Schema> extends SchemaAllocator<T> {

	T malloc(int index);

	T calloc(int index);

	void clear(int index);

	long byteSize();

	long size();
}

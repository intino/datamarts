package systems.intino.datamarts.led.allocators.indexed;

import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.SchemaAllocator;

public interface IndexedAllocator<T extends Schema> extends SchemaAllocator<T> {

	T malloc(int index);

	T calloc(int index);

	void clear(int index);

	long byteSize();

	long size();
}

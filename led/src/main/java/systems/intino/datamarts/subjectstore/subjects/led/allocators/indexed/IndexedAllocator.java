package systems.intino.datamarts.subjectstore.subjects.led.allocators.indexed;

import systems.intino.datamarts.subjectstore.subjects.led.Schema;
import systems.intino.datamarts.subjectstore.subjects.led.allocators.SchemaAllocator;

public interface IndexedAllocator<T extends Schema> extends SchemaAllocator<T> {

	T malloc(int index);

	T calloc(int index);

	void clear(int index);

	long byteSize();

	long size();
}

package systems.intino.datamarts.led.util;

public interface OffHeapObject {

	long NULL = 0L;

	long address();

	long byteSize();

	long baseOffset();

	default boolean isNull() {
		return address() == NULL;
	}

	default boolean notNull() {
		return address() != NULL;
	}
}

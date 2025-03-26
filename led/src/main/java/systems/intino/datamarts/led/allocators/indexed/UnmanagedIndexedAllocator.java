package systems.intino.datamarts.led.allocators.indexed;

import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.SchemaFactory;
import systems.intino.datamarts.led.buffers.store.NativePointerStore;
import systems.intino.datamarts.led.util.memory.MemoryUtils;
import systems.intino.datamarts.led.util.memory.ModifiableMemoryAddress;
import systems.intino.datamarts.led.util.memory.NativePointerCleaner;

import java.lang.ref.Cleaner;

import static systems.intino.datamarts.led.util.memory.MemoryUtils.NULL;
import static systems.intino.datamarts.led.util.memory.MemoryUtils.memset;

public class UnmanagedIndexedAllocator<T extends Schema> implements IndexedAllocator<T> {

	private static final Cleaner CLEANER = Cleaner.create();

	private NativePointerStore store;
	private final ModifiableMemoryAddress address;
	private final int elementSize;
	private final SchemaFactory<T> factory;
	private final Cleaner.Cleanable cleanable;

	public UnmanagedIndexedAllocator(long baseAddress, long baseOffset, long size, int elementSize, Class<T> schemaClass) {
		this.elementSize = elementSize;
		this.factory = Schema.factoryOf(schemaClass);
		this.address = new ModifiableMemoryAddress(baseAddress);
		store = new NativePointerStore(address, baseOffset, size);
		cleanable = CLEANER.register(this, new NativePointerCleaner(address));
	}

	public UnmanagedIndexedAllocator(long elementsCount, int elementSize, Class<T> schemaClass) {
		this.elementSize = elementSize;
		this.factory = Schema.factoryOf(schemaClass);
		final long size = elementsCount * elementSize;
		address = new ModifiableMemoryAddress(MemoryUtils.malloc(size));
		store = new NativePointerStore(address, 0, size);
		cleanable = CLEANER.register(this, new NativePointerCleaner(address));
	}

	@Override
	public T malloc(int index) {
		final int offset = index * elementSize;
		return factory.newInstance(store.slice(offset, elementSize));
	}

	@Override
	public T calloc(int index) {
		T instance = malloc(index);
		instance.clear();
		return instance;
	}

	@Override
	public void clear(int index) {
		memset(address.get() + (long) index * elementSize, elementSize, 0);
	}

	@Override
	public long byteSize() {
		return store.byteSize();
	}

	@Override
	public long size() {
		return (byteSize() / elementSize);
	}

	@Override
	public T malloc() {
		return malloc(0);
	}

	@Override
	public T calloc() {
		return calloc(0);
	}

	@Override
	public int schemaSize() {
		return elementSize;
	}

	@Override
	public void clear() {
		// store.clear();
	}

	@Override
	public void free() {
		if (address.get() != NULL) {
			cleanable.clean();
			store = null;
		}
	}

	@Override
	public Class<T> schemaClass() {
		return factory.schemaClass();
	}

}

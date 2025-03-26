package systems.intino.datamarts.led.allocators.indexed;

import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.SchemaFactory;
import systems.intino.datamarts.led.buffers.store.ByteBufferStore;
import systems.intino.datamarts.led.util.memory.MemoryUtils;
import systems.intino.datamarts.led.util.memory.ModifiableMemoryAddress;

import java.nio.ByteBuffer;

import static systems.intino.datamarts.led.util.memory.MemoryUtils.NULL;
import static systems.intino.datamarts.led.util.memory.MemoryUtils.allocBuffer;

public class ManagedIndexedAllocator<T extends Schema> implements IndexedAllocator<T> {

	private ByteBufferStore store;
	private final ModifiableMemoryAddress address;
	private final int schemaSize;
	private final SchemaFactory<T> factory;

	public ManagedIndexedAllocator(ByteBuffer buffer, int baseOffset, int size, int schemaSize, Class<T> schemaClass) {
		this.schemaSize = schemaSize;
		this.factory = Schema.factoryOf(schemaClass);
		address = ModifiableMemoryAddress.of(buffer);
		store = new ByteBufferStore(buffer, address, baseOffset, size);
	}

	public ManagedIndexedAllocator(long elementsCount, int schemaSize, SchemaFactory<T> factory) {
		this.schemaSize = schemaSize;
		this.factory = factory;
		final long size = elementsCount * schemaSize;
		if (size > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Size too large for ManagedIndexedAllocator");
		}
		ByteBuffer buffer = allocBuffer((int) size);
		address = ModifiableMemoryAddress.of(buffer);
		store = new ByteBufferStore(buffer, address, 0, (int) size);
	}

	@Override
	public T malloc(int index) {
		final int offset = index * schemaSize;
		return factory.newInstance(store.slice(offset, schemaSize));
	}

	@Override
	public T calloc(int index) {
		T instance = malloc(index);
		instance.clear();
		return instance;
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
	public void clear(int index) {
		// memset(address.get() + index * schemaSize, schemaSize, 0);
	}

	@Override
	public long byteSize() {
		return store.byteSize();
	}

	@Override
	public long size() {
		return (int) (byteSize() / schemaSize);
	}

	@Override
	public int schemaSize() {
		return schemaSize;
	}

	@Override
	public void clear() {
		store.clear();
	}

	@Override
	public void free() {
		if (address.get() != NULL) {
			MemoryUtils.free(store.storeImpl());
			store = null;
			address.set(NULL);
		}
	}

	@Override
	public Class<T> schemaClass() {
		return factory.schemaClass();
	}
}

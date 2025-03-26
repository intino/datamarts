package systems.intino.datamarts.led.allocators.indexed;

import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.SchemaFactory;
import systems.intino.datamarts.led.buffers.store.ByteBufferStore;
import systems.intino.datamarts.led.buffers.store.ByteStore;
import systems.intino.datamarts.led.util.memory.MemoryUtils;
import systems.intino.datamarts.led.util.memory.ModifiableMemoryAddress;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static systems.intino.datamarts.led.util.memory.MemoryUtils.*;

public class ListAllocator<T extends Schema> implements IndexedAllocator<T> {

	private final List<ModifiableMemoryAddress> addresses;
	private final int elementSize;
	private final SchemaFactory<T> factory;
	private final int elementsCountPerBuffer;
	private final Queue<Integer> freeIndices;
	private List<ByteBufferStore> stores;
	private int lastIndex;

	public ListAllocator(long elementsCountPerBuffer, int schemaSize, Class<T> schemaClass) {
		if (elementsCountPerBuffer * schemaSize > Integer.MAX_VALUE)
			throw new IllegalArgumentException("Size too large for ByteBufferStore");
		this.elementSize = schemaSize;
		this.factory = Schema.factoryOf(schemaClass);
		this.elementsCountPerBuffer = (int) elementsCountPerBuffer;
		this.stores = new ArrayList<>();
		this.addresses = new ArrayList<>();
		this.freeIndices = new ArrayDeque<>();
	}

	@Override
	public T malloc(int index) {
		while (index > lastPossibleIndex()) allocateNewByteStore();
		final int storeIndex = storeIndex(index);
		final ByteStore store = stores.get(storeIndex);
		final int relativeIndex = storeRelativeIndex(index);
		final int offset = relativeIndex * elementSize;
		return factory.newInstance(store.slice(offset, elementSize));
	}

	@Override
	public T malloc() {
		int index;
		if (!freeIndices.isEmpty()) index = freeIndices.poll();
		else index = lastIndex++;
		return malloc(index);
	}

	@Override
	public T calloc() {
		T instance = malloc();
		instance.clear();
		return instance;
	}

	@Override
	public T calloc(int index) {
		T instance = malloc(index);
		instance.clear();
		return instance;
	}

	@Override
	public void clear(int index) {
		if (index > lastIndex) return;
		final int storeIndex = storeIndex(index);
		final int relativeIndex = storeRelativeIndex(index);
		memset(addresses.get(storeIndex).get() + (long) relativeIndex * elementSize, elementSize, 0);
	}

	public void free(int index) {
		if (index > lastIndex) return;
		if (index == lastIndex) {
			--lastIndex;
			return;
		}
		freeIndices.add(index);
	}

	public void free(Schema schema) {
		int index;
		final long address = schema.address();
		index = stores.stream().takeWhile(store -> store.address() != address).mapToInt(this::countElements).sum();
		index += schema.baseOffset() / elementSize;
		free(index);
		schema.invalidate();
	}

	private int lastPossibleIndex() {
		return (stores.size() * elementsCountPerBuffer) - 1;
	}

	@Override
	public long byteSize() {
		return stores.stream().mapToLong(ByteStore::byteSize).sum();
	}

	@Override
	public long size() {
		return lastIndex;
	}

	public int capacity() {
		return stores.stream().mapToInt(this::countElements).sum();
	}

	@Override
	public int schemaSize() {
		return elementSize;
	}

	@Override
	public void clear() {
		lastIndex = 0;
		freeIndices.clear();
	}

	@Override
	public void free() {
		if (stores != null) {
			for (int i = 0; i < stores.size(); i++) {
				ByteBufferStore store = stores.get(i);
				ModifiableMemoryAddress address = addresses.get(i);
				if (address.notNull()) {
					MemoryUtils.free(store.storeImpl());
					address.set(NULL);
				}
			}
			stores = null;
			lastIndex = Integer.MIN_VALUE;
		}
	}

	@Override
	public Class<T> schemaClass() {
		return factory.schemaClass();
	}

	private int storeIndex(int elementIndex) {
		long end = 0;
		for (int i = 0; i < stores.size(); i++) {
			ByteStore store = stores.get(i);
			end += store.byteSize() / elementSize;
			if (elementIndex < end) return i;
		}
		throw new IndexOutOfBoundsException(elementIndex + " out of " + end);
	}

	private int storeRelativeIndex(int elementIndex) {
		return elementIndex % elementsCountPerBuffer;
	}

	private int countElements(ByteStore store) {
		return (int) (store.byteSize() / elementSize);
	}

	private void allocateNewByteStore() {
		ByteBuffer buffer = allocBuffer((long) elementsCountPerBuffer * elementSize);
		ModifiableMemoryAddress address = ModifiableMemoryAddress.of(buffer);
		ByteBufferStore store = new ByteBufferStore(buffer, address, buffer.position(), buffer.capacity());
		stores.add(store);
		addresses.add(address);
	}
}
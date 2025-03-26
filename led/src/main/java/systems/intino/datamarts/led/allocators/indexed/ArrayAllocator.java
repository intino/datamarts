package systems.intino.datamarts.led.allocators.indexed;

import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.SchemaFactory;
import systems.intino.datamarts.led.buffers.store.ByteBufferStore;
import systems.intino.datamarts.led.buffers.store.ByteStore;
import systems.intino.datamarts.led.util.memory.MemoryUtils;
import systems.intino.datamarts.led.util.memory.ModifiableMemoryAddress;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static systems.intino.datamarts.led.util.memory.MemoryUtils.*;

public class ArrayAllocator<T extends Schema> implements IndexedAllocator<T> {

	private ByteBufferStore[] stores;
	private final ModifiableMemoryAddress[] addresses;
	private final Map<Integer, Integer> indices;
	private final int elementSize;
	private final SchemaFactory<T> factory;

	public ArrayAllocator(int capacity, int elementsPerBuffer, int elementSize, Class<T> schemaClass) {
		this(generateBuffers(capacity, elementsPerBuffer * elementSize), elementSize, schemaClass);
	}

	public ArrayAllocator(List<ByteBuffer> buffers, int elementSize, Class<T> schemaClass) {
		this.elementSize = elementSize;
		this.factory = Schema.factoryOf(schemaClass);
		addresses = new ModifiableMemoryAddress[buffers.size()];
		stores = new ByteBufferStore[buffers.size()];
		indices = new HashMap<>(buffers.size());
		int offset = 0;
		for (int i = 0; i < buffers.size(); i++) {
			ByteBuffer buffer = buffers.get(i);
			ModifiableMemoryAddress address = ModifiableMemoryAddress.of(buffer);
			ByteBufferStore store = new ByteBufferStore(buffer, address, buffer.position(), buffer.limit());
			addresses[i] = address;
			stores[i] = store;
			indices.put(i, offset);
			offset += countElements(store);
		}
	}

	@Override
	public T malloc(int index) {
		final int storeIndex = storeIndex(index);
		final ByteStore store = stores[storeIndex];
		final int relativeIndex = storeRelativeIndex(index, storeIndex);
		final int offset = relativeIndex * elementSize;
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
		final int storeIndex = storeIndex(index);
		final int relativeIndex = storeRelativeIndex(index, storeIndex);
		memset(addresses[storeIndex].get() + (long) relativeIndex * elementSize, elementSize, 0);
	}

	@Override
	public long byteSize() {
		return Arrays.stream(stores).mapToLong(ByteStore::byteSize).sum();
	}

	@Override
	public long size() {
		return Arrays.stream(stores).mapToInt(this::countElements).sum();
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
		// Arrays.stream(stores).forEach(ByteBufferStore::clear);
	}

	@Override
	public void free() {
		if (stores != null) {
			for (int i = 0; i < stores.length; i++) {
				ByteBufferStore store = stores[i];
				ModifiableMemoryAddress address = addresses[i];
				if (address.notNull()) {
					MemoryUtils.free(store.storeImpl());
					address.set(NULL);
				}
			}
			stores = null;
		}
	}

	@Override
	public Class<T> schemaClass() {
		return factory.schemaClass();
	}

	private int storeIndex(int elementIndex) {

		long end = 0;

		for (int i = 0; i < stores.length; i++) {
			ByteStore store = stores[i];
			end += store.byteSize() / elementSize;
			if (elementIndex < end) {
				return i;
			}
		}

		throw new IndexOutOfBoundsException(elementIndex + " out of " + end);
	}

	private int storeRelativeIndex(int elementIndex, int storeIndex) {
		final int offset = indices.get(storeIndex);
		return elementIndex - offset;
	}

	private int countElements(ByteStore store) {
		return (int) (store.byteSize() / elementSize);
	}

	private static List<ByteBuffer> generateBuffers(int capacity, int bufferSize) {
		return IntStream.range(0, capacity)
				.mapToObj(i -> allocBuffer(bufferSize))
				.collect(Collectors.toList());
	}
}

package systems.intino.datamarts.led.allocators.stack;

import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.buffers.store.ByteBufferStore;
import systems.intino.datamarts.led.buffers.store.ByteStore;
import systems.intino.datamarts.led.buffers.store.NativePointerStore;
import systems.intino.datamarts.led.util.memory.MemoryUtils;
import systems.intino.datamarts.led.util.memory.ModifiableMemoryAddress;

import java.nio.ByteBuffer;

import static systems.intino.datamarts.led.util.memory.MemoryUtils.allocBuffer;

public final class StackAllocators {

	public static <E extends Schema> StackAllocator<E> unmanagedStackAllocator(int elementSize, long elementCount, Class<E> schemaClass) {

		final long size = elementSize * elementCount;
		final long ptr = MemoryUtils.malloc(size);
		ModifiableMemoryAddress address = new ModifiableMemoryAddress(ptr);
		ByteStore store = new NativePointerStore(address, 0, size);
		return new SingleStackAllocator<>(store, address, elementSize, schemaClass);
	}

	public static <E extends Schema> StackAllocator<E> managedStackAllocator(int elementSize, long elementCount, Class<E> schemaClass) {
		if (elementCount < 0) throw new IllegalArgumentException("Element count is negative");
		if (elementCount > Integer.MAX_VALUE)
			throw new IllegalArgumentException("Element Count too large for managed byte store");
		final long size = elementSize * elementCount;
		if (size > Integer.MAX_VALUE) throw new IllegalArgumentException("Size too large for managed byte store");
		final ByteBuffer buffer = allocBuffer((int) size);
		ModifiableMemoryAddress address = ModifiableMemoryAddress.of(buffer);
		ByteStore store = new ByteBufferStore(buffer, address, 0, (int) size);
		return new SingleStackAllocator<>(store, address, elementSize, schemaClass);
	}

	public static <E extends Schema> StackAllocator<E> managedStackAllocatorFromBuffer(int elementSize, ByteBuffer buffer, Class<E> schemaClass) {
		ModifiableMemoryAddress address = ModifiableMemoryAddress.of(buffer);
		ByteStore store = new ByteBufferStore(buffer, address, 0, buffer.remaining());
		return new SingleStackAllocator<>(store, address, elementSize, schemaClass);
	}

	private StackAllocators() {
	}
}

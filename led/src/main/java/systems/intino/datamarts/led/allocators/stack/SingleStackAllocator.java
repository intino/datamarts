package systems.intino.datamarts.led.allocators.stack;

import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.SchemaFactory;
import systems.intino.datamarts.led.buffers.store.ByteStore;
import systems.intino.datamarts.led.exceptions.StackAllocatorOverflowException;
import systems.intino.datamarts.led.exceptions.StackAllocatorUnderflowException;
import systems.intino.datamarts.led.util.memory.MemoryUtils;
import systems.intino.datamarts.led.util.memory.ModifiableMemoryAddress;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static systems.intino.datamarts.led.util.memory.MemoryUtils.NULL;

public class SingleStackAllocator<T extends Schema> implements StackAllocator<T> {

	private final int elementSize;
	private ByteStore stack;
	private final ModifiableMemoryAddress address;
	private final AtomicLong stackPointer;
	private final SchemaFactory<T> factory;

	public SingleStackAllocator(ByteStore store, ModifiableMemoryAddress address, int elementSize, Class<T> schemaClass) {
		this.elementSize = elementSize;
		this.stack = store;
		this.factory = Schema.factoryOf(schemaClass);
		stackPointer = new AtomicLong();
		this.address = address;
	}

	public long address() {
		return address.get();
	}

	public long stackSize() {
		return stack.byteSize();
	}

	@Override
	public long stackPointer() {
		return stackPointer.get();
	}

	@Override
	public long remainingBytes() {
		return stackSize() - stackPointer.get();
	}

	@Override
	public long size() {
		return stackSize() / elementSize;
	}

	@Override
	public synchronized T malloc() {
		if (remainingBytes() < elementSize) {
			throw new StackAllocatorOverflowException();
		}
		ByteStore store = stack.slice(stackPointer.getAndAdd(elementSize), elementSize);
		return factory.newInstance(store);
	}

	@Override
	public synchronized T calloc() {
		final T instance = malloc();
		instance.clear();
		return instance;
	}

	@Override
	public int schemaSize() {
		return elementSize;
	}

	@Override
	public synchronized void pop() {
		if (stackPointer.get() == 0) {
			throw new StackAllocatorUnderflowException();
		}
		stackPointer.addAndGet(-elementSize);
	}

	@Override
	public synchronized void clear() {
		stackPointer.set(0);
	}

	@Override
	public synchronized void free() {
		if (address.notNull()) {
			final Object storeImpl = stack.storeImpl();
			if (storeImpl instanceof ByteBuffer) {
				MemoryUtils.free((ByteBuffer) storeImpl);
			} else if (storeImpl instanceof Long) {
				MemoryUtils.free((long) storeImpl);
			}
			address.set(NULL);
			stack = null;
		}
	}

	@Override
	public Class<T> schemaClass() {
		return factory.schemaClass();
	}
}

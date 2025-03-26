package systems.intino.datamarts.led.allocators.stack;

import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.SchemaFactory;
import systems.intino.datamarts.led.exceptions.StackAllocatorUnderflowException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class StackListAllocator<T extends Schema> implements StackAllocator<T> {

	private static final int DEFAULT_INITIAL_STACK_COUNT = 1;


	private final List<StackAllocator<T>> stackAllocators;
	private final AtomicInteger currentStackAllocator;
	private final int elementsPerStack;
	private final int elementSize;
	private final SchemaFactory<T> schemaFactory;
	private final StackAllocatorFactory<T> stackAllocatorFactory;

	public StackListAllocator(int initialStackCount, int elementsPerStack, int elementSize,
                              Class<T> schemaClass, StackAllocatorFactory<T> stackAllocatorFactory) {

		this.stackAllocators = new ArrayList<>();
		currentStackAllocator = new AtomicInteger(0);
		this.elementsPerStack = elementsPerStack;
		this.elementSize = elementSize;
		this.schemaFactory = Schema.factoryOf(schemaClass);
		this.stackAllocatorFactory = stackAllocatorFactory;

		reserve(initialStackCount);
	}

	public StackListAllocator(int elementsPerStack, int elementSize, Class<T> schemaClass, StackAllocatorFactory<T> stackAllocatorFactory) {

		this(DEFAULT_INITIAL_STACK_COUNT, elementsPerStack, elementSize, schemaClass, stackAllocatorFactory);
	}

	@Override
	public long stackPointer() {
		return currentStackAllocator().stackPointer();
	}

	@Override
	public long remainingBytes() {
		return currentStackAllocator().remainingBytes();
	}

	@Override
	public long size() {
		return (currentStackAllocator.get() * stackSize()) / elementSize;
	}

	@Override
	public synchronized T malloc() {
		if (stackAllocators.isEmpty()) {
			allocateNewStack();
		} else if (remainingBytes() == 0) {
			int current = currentStackAllocator.incrementAndGet();
			if (current == stackAllocators.size()) {
				allocateNewStack();
			}
		}
		return currentStackAllocator().malloc();
	}

	@Override
	public synchronized T calloc() {
		if (stackAllocators.isEmpty()) {
			allocateNewStack();
		} else if (remainingBytes() == 0) {
			int current = currentStackAllocator.incrementAndGet();
			if (current == stackAllocators.size()) {
				allocateNewStack();
			}
		}
		return currentStackAllocator().calloc();
	}

	@Override
	public int schemaSize() {
		return elementSize;
	}

	@Override
	public synchronized void pop() {
		if (stackPointer() == 0) {
			if (currentStackAllocator.get() == 0) {
				throw new StackAllocatorUnderflowException();
			}
			currentStackAllocator.decrementAndGet();
		}
		currentStackAllocator().pop();
	}

	public void reserve(int stackCount) {
		for (int i = 0; i < stackCount; i++) {
			allocateNewStack();
		}
	}

	@Override
	public synchronized void clear() {
		stackAllocators.forEach(StackAllocator::clear);
		currentStackAllocator.set(0);
	}

	@Override
	public synchronized void free() {
		stackAllocators.forEach(StackAllocator::free);
		stackAllocators.clear();
		currentStackAllocator.set(0);
	}

	@Override
	public Class<T> schemaClass() {
		return schemaFactory.schemaClass();
	}

	@Override
	public long address() {
		return currentStackAllocator().address();
	}

	@Override
	public long stackSize() {
		return currentStackAllocator().stackSize();
	}

	private void allocateNewStack() {
		stackAllocators.add(stackAllocatorFactory.create(elementSize, elementsPerStack, schemaFactory.schemaClass()));
	}

	private StackAllocator<T> currentStackAllocator() {
		if (stackAllocators.isEmpty()) {
			allocateNewStack();
		}
		return stackAllocators.get(currentStackAllocator.get());
	}
}

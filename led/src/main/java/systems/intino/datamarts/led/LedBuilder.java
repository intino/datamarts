package systems.intino.datamarts.led;

import systems.intino.datamarts.led.allocators.indexed.IndexedAllocator;
import systems.intino.datamarts.led.allocators.indexed.ListAllocator;
import systems.intino.datamarts.led.leds.ArrayLed;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;

/**
 * Consider using {@link HeapLedStreamBuilder} first when performance is critical.
 *
 * */
public class LedBuilder<T extends Schema> implements Led.Builder<T> {

	public static final int DEFAULT_INITIAL_CAPACITY = 10_000;
	public static final float GROW_FACTOR = 1.5f;


	private final Class<T> schemaClass;
	private final IndexedAllocator<T> allocator;
	private final AtomicBoolean closed;
	private T[] sortedTransactions;
	private int size;

	public LedBuilder(Class<T> schemaClass) {
		this(schemaClass, allocator(Schema.sizeOf(schemaClass), schemaClass));
	}

	@SuppressWarnings("unchecked")
	public LedBuilder(Class<T> schemaClass, IndexedAllocator<T> allocator) {
		this.schemaClass = requireNonNull(schemaClass);
		this.allocator = requireNonNull(allocator);
		this.sortedTransactions = (T[]) new Schema[DEFAULT_INITIAL_CAPACITY];
		this.closed = new AtomicBoolean(false);
	}

	@Override
	public Class<T> schemaClass() {
		return schemaClass;
	}

	@Override
	public int schemaSize() {
		return allocator.schemaSize();
	}

	@Override
	public Led.Builder<T> create(Consumer<T> initializer) {
		if(closed.get()) throw new IllegalStateException("LedBuilder is closed because build was called.");
		T schema = allocator.malloc();
		initializer.accept(schema);
		putInSortedList(schema);
		return this;
	}

	private void putInSortedList(T schema) {
		if(size >= sortedTransactions.length)
			grow();

		if(size == 0)
			sortedTransactions[size++] = schema;
		else
			insertSorted(schema);
	}

	private void insertSorted(T schema) {
		int index = Arrays.binarySearch(sortedTransactions, 0, size, schema);
		if(index < 0) index = -index - 1;
		arraycopy(sortedTransactions, index, sortedTransactions, index + 1, size - index);
		sortedTransactions[index] = schema;
		++size;
	}

	private void grow() {
		sortedTransactions = Arrays.copyOf(sortedTransactions, Math.round(size * GROW_FACTOR));
	}

	public Led<T> build() {
		if(!closed.compareAndSet(false, true)) throw new IllegalStateException("This LedBuilder has already been closed");
		return new ArrayLed<>(schemaClass, sortedTransactions, size);
	}

	private static <T extends Schema> IndexedAllocator<T> allocator(int schemaSize, Class<T> schemaClass) {
		return new ListAllocator<>(DEFAULT_INITIAL_CAPACITY, schemaSize, schemaClass);
	}
}

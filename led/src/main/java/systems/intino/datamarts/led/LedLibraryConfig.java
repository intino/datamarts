package systems.intino.datamarts.led;

import systems.intino.datamarts.led.util.memory.AllocationInfo;

import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public final class LedLibraryConfig {

	private LedLibraryConfig() {}

	public static final Variable<ByteOrder> BYTE_ORDER = new Variable<>(ByteOrder.nativeOrder());
	public static final Variable<Boolean> USE_MEMORY_TRACKER = new Variable<>(false);
	public static final Variable<Consumer<Long>> BEFORE_ALLOCATION_CALLBACK = new Variable<>();
	public static final Variable<Consumer<AllocationInfo>> ALLOCATION_CALLBACK = new Variable<>();
	public static final Variable<Consumer<AllocationInfo>> FREE_CALLBACK = new Variable<>();
	public static final Variable<Integer> DEFAULT_BUFFER_SIZE = new Variable<>(1024);
	public static final Variable<Boolean> INPUT_LEDSTREAM_CONCURRENCY_ENABLED = new Variable<>(false);
	public static final Variable<Boolean> CHECK_SERIAL_ID = new Variable<>(true);


	public static final class Variable<T> {

		private final AtomicReference<T> value;

		public Variable() {
			value = new AtomicReference<>();
		}

		public Variable(T defaultValue) {
			this();
			set(defaultValue);
		}

		public boolean isEmpty() {
			return value.get() == null;
		}

		public T get() {
			return value.get();
		}

		public T getOrDefault(T newValue) {
			if (isEmpty()) {
				set(newValue);
			}
			return value.get();
		}

		public Variable<T> set(T value) {
			this.value.set(requireNonNull(value));
			return this;
		}
	}
}

package systems.intino.datamarts.led;

import systems.intino.datamarts.led.allocators.SchemaAllocator;
import systems.intino.datamarts.led.allocators.stack.StackAllocators;
import systems.intino.datamarts.led.allocators.stack.StackListAllocator;
import systems.intino.datamarts.led.leds.IteratorLedStream;
import systems.intino.datamarts.led.util.iterators.IteratorUtils;
import systems.intino.datamarts.led.util.iterators.MergedIterator;

import java.io.File;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static systems.intino.datamarts.led.Schema.idOf;
import static systems.intino.datamarts.led.Schema.sizeOf;

public interface LedStream<T extends Schema> extends Iterator<T>, AutoCloseable {

	static <T extends Schema> LedStream<T> empty(Class<T> schemaClass) {
		return empty(schemaClass, Schema.sizeOf(schemaClass));
	}

	static <T extends Schema> LedStream<T> empty(Class<T> schemaClass, int schemaSize) {
		return IteratorLedStream.fromStream(schemaClass, schemaSize, Stream.empty());
	}

	static<T extends Schema> LedStream<T> fromLed(Led<T> led) {
		return led.toLedStream();
	}

	static <T extends Schema> LedStream<T> fromStream(Class<T> schemaClass, Stream<T> stream) {
		return IteratorLedStream.fromStream(schemaClass, stream);
	}

	static <T extends Schema> LedStream<T> of(Class<T> schemaClass, T... schemas) {
		return fromStream(schemaClass, Arrays.stream(schemas));
	}

	static <T extends Schema> LedStream<T> singleton(Class<T> schemaClass, T schema) {
		return fromStream(schemaClass, Stream.of(schema));
	}

	static <T extends Schema> LedStream<T> merged(Stream<LedStream<T>> ledStreams) {
		return merged(ledStreams.iterator());
	}

	static <T extends Schema> LedStream<T> merged(Iterator<LedStream<T>> iterator) {
		return iterator.next().merge(IteratorUtils.streamOf(iterator));
	}

	static <T extends Schema> Builder<T> builder(Class<T> schemaClass) {
		return new HeapLedStreamBuilder<>(schemaClass);
	}

	static <T extends Schema> Builder<T> builder(Class<T> schemaClass, File tempDirectory) {
		return new HeapLedStreamBuilder<>(schemaClass, tempDirectory);
	}

	static <T extends Schema> Builder<T> builder(Class<T> schemaClass, int numElementsPerBlock) {
		return new HeapLedStreamBuilder<>(schemaClass, numElementsPerBlock);
	}

	static <T extends Schema> Builder<T> builder(Class<T> schemaClass, int numElementsPerBlock, File tempDirectory) {
		return new HeapLedStreamBuilder<>(schemaClass, numElementsPerBlock, tempDirectory);
	}

	int schemaSize();

	@Override
	boolean hasNext();

	@Override
	T next();

	default Spliterator<T> spliterator() {
		return Spliterators.spliteratorUnknownSize(this, Spliterator.SORTED);
	}

	default Iterable<T> iterable() {
		return () -> LedStream.this;
	}

	default Stream<T> asJavaStream() {
		return StreamSupport.stream(spliterator(), false);
	}

	default LedStream<T> filter(Predicate<T> condition) {
		return new Filter<>(this, condition);
	}

	default LedStream<T> peek(Consumer<T> consumer) {
		return new Peek<>(this, consumer);
	}

	default <R extends Schema> LedStream<R> map(SchemaAllocator<R> allocator, BiConsumer<T, R> mapper) {
		return new Map<>(this, allocator, mapper);
	}

	default <R extends Schema> LedStream<R> map(int rSize, Class<R> newType, BiConsumer<T, R> mapper) {
		return new Map<>(this, rSize, newType, mapper);
	}

	default <R extends Schema> LedStream<R> map(Class<R> newType, BiConsumer<T, R> mapper) {
		return new Map<>(this, sizeOf(newType), newType, mapper);
	}

	default <R> Stream<R> mapToObj(Function<T, R> mapper) {
		return asJavaStream().map(mapper);
	}

	default LedStream<T> merge(LedStream<T> other) {
		return new Merge<>(this, other);
	}

	default LedStream<T> merge(Stream<LedStream<T>> others) {
		return new Merge<>(this, others);
	}

	default <O extends Schema> LedStream<T> removeAll(LedStream<O> other) {
		return new RemoveAll<>(this, other);
	}

	default LedStream<T> removeAll(Iterable<Long> other) {
		return new RemoveAll<>(this, other.iterator());
	}

	default LedStream<T> removeAll(Iterator<Long> other) {
		return new RemoveAll<>(this, other);
	}

	default <O extends Schema> LedStream<T> retainAll(LedStream<O> other) {
		return new RetainAll<>(this, other);
	}

	default LedStream<T> retainAll(Iterable<Long> other) {
		return new RetainAll<>(this, other.iterator());
	}

	default LedStream<T> retainAll(Iterator<Long> other) {
		return new RetainAll<>(this, other);
	}

	default Optional<T> findFirst() {
		return hasNext() ? Optional.ofNullable(next()) : Optional.empty();
	}

	default Optional<T> findLast() {
		T last = null;
		while(hasNext()) {
			last = next();
		}
		return Optional.ofNullable(last);
	}

	default boolean allMatch(Predicate<T> condition) {
		while(hasNext()) {
			if(!condition.test(next())) {
				return false;
			}
		}
		return true;
	}

	default boolean anyMatch(Predicate<T> condition) {
		while(hasNext()) {
			if(condition.test(next())) {
				return true;
			}
		}
		return false;
	}

	default boolean noneMatch(Predicate<T> condition) {
		while(hasNext()) {
			if(condition.test(next())) {
				return false;
			}
		}
		return true;
	}

	default void forEach(Consumer<T> consumer) {
		while(hasNext()) {
			consumer.accept(next());
		}
	}

	default void serialize(File file) {
		LedWriter ledWriter = new LedWriter(file);
		ledWriter.write(this);
	}

	default void serializeUncompressed(File file) {
		LedWriter ledWriter = new LedWriter(file);
		ledWriter.writeUncompressed(this);
	}

	default LedStream<T> onClose(Runnable onClose) {
		return this;
	}

	Class<T> schemaClass();

	default UUID serialUUID() {
		return Schema.getSerialUUID(schemaClass());
	}

	abstract class LedStreamOperation<T extends Schema, R extends Schema> implements LedStream<R> {

		protected final LedStream<T> source;
		private Runnable onClose;
		private boolean closed;

		public LedStreamOperation(LedStream<T> source) {
			this.source = requireNonNull(source);
		}

		@Override
		public int schemaSize() {
			return source.schemaSize();
		}

		@Override
		public LedStream<R> onClose(Runnable onClose) {
			this.onClose = onClose;
			return this;
		}

		@Override
		public void close() throws Exception {
			if(closed) {
				return;
			}
			if(onClose != null) {
				onClose.run();
			}
			source.close();
			closed = true;
		}
	}

	class Filter<T extends Schema> extends LedStreamOperation<T, T> {

		private final Predicate<T> condition;
		private T current;

		public Filter(LedStream<T> source, Predicate<T> condition) {
			super(source);
			this.condition = requireNonNull(condition);
		}

		@Override
		public boolean hasNext() {
			if(current == null) {
				advanceToNextElement();
			}
			return current != null;
		}

		private void advanceToNextElement() {
			while(source.hasNext()) {
				final T next = source.next();
				if(condition.test(next)) {
					current = next;
					break;
				}
			}
		}

		@Override
		public T next() {
			if(!hasNext())
				throw new NoSuchElementException();
			final T next = current;
			current = null;
			return next;
		}

		@Override
		public Class<T> schemaClass() {
			return source.schemaClass();
		}
	}

	class Peek<T extends Schema> extends LedStreamOperation<T, T> {

		private final Consumer<T> consumer;

		public Peek(LedStream<T> source, Consumer<T> consumer) {
			super(source);
			this.consumer = requireNonNull(consumer);
		}

		@Override
		public boolean hasNext() {
			return source.hasNext();
		}

		@Override
		public T next() {
			final T next = source.next();
			consumer.accept(next);
			return next;
		}

		@Override
		public Class<T> schemaClass() {
			return source.schemaClass();
		}
	}

	class RemoveAll<T extends Schema> extends LedStreamOperation<T, T> {

		private final Iterator<Long> other;
		private T sourceCurrent;
		private Long otherCurrentId;

		public RemoveAll(LedStream<T> source, LedStream<?> other) {
			this(source, other.mapToObj(Schema::idOf).iterator());
		}

		public RemoveAll(LedStream<T> source, Iterator<Long> idIterator) {
			super(source);
			this.other = requireNonNull(idIterator);
		}

		@Override
		public boolean hasNext() {
			if(sourceCurrent == null) {
				advanceToNextElement();
			}
			return sourceCurrent != null;
		}

		private void advanceToNextElement() {
			if(!source.hasNext()) {
				return;
			}
			if(!other.hasNext()) {
				sourceCurrent = source.next();
				otherCurrentId = null;
				return;
			}

			T sourceElement = source.next();
			if(otherCurrentId == null) {
				otherCurrentId = other.next();
			}

			if(idOf(sourceElement) < otherCurrentId) {
				sourceCurrent = sourceElement;
				return;
			}
			while(idOf(sourceElement) > otherCurrentId) {
				if(other.hasNext()) {
					otherCurrentId = other.next();
				} else {
					sourceCurrent = sourceElement;
					otherCurrentId = null;
					return;
				}
			}

			do {
				long id = idOf(sourceElement);
				final int comparison = Long.compare(id, otherCurrentId);
				if(comparison == 0) {
					if(source.hasNext()) {
						sourceElement = source.next();
					} else {
						sourceCurrent = null;
						return;
					}
				} else if(comparison > 0) {
					if(other.hasNext()) {
						otherCurrentId = other.next();
					} else {
						otherCurrentId = null;
						break;
					}
				}

			} while(idOf(sourceElement) >= otherCurrentId);

			sourceCurrent = sourceElement;
		}

		@Override
		public T next() {
			if(!hasNext())
				throw new NoSuchElementException();
			final T next = sourceCurrent;
			sourceCurrent = null;
			return next;
		}

		@Override
		public Class<T> schemaClass() {
			return source.schemaClass();
		}
	}

	class Merge<T extends Schema> extends LedStreamOperation<T, T> {

		private final MergedIterator<T> mergedIterator;

		public Merge(LedStream<T> source, LedStream<T> other) {
			super(source);
			mergedIterator = new MergedIterator<>(Stream.of(source, requireNonNull(other)), Comparator.comparingLong(Schema::idOf));
		}

		public Merge(LedStream<T> source, Stream<LedStream<T>> others) {
			super(source);
			mergedIterator = new MergedIterator<>(Stream.concat(Stream.of(source), others), Comparator.comparingLong(Schema::idOf));
		}

		@Override
		public boolean hasNext() {
			return mergedIterator.hasNext();
		}

		@Override
		public T next() {
			return mergedIterator.next();
		}

		@Override
		public Class<T> schemaClass() {
			return source.schemaClass();
		}
	}

	class RetainAll<T extends Schema> extends LedStreamOperation<T, T> {

		private final Iterator<Long> other;
		private T current;

		public RetainAll(LedStream<T> source, LedStream<?> other) {
			this(source, other.mapToObj(Schema::idOf).iterator());
		}

		public RetainAll(LedStream<T> source, Iterator<Long> idIterator) {
			super(source);
			this.other = requireNonNull(idIterator);
		}

		@Override
		public boolean hasNext() {
			if(current == null) {
				advanceToNextElement();
			}
			return current != null;
		}

		private void advanceToNextElement() {
			if(!source.hasNext() || !other.hasNext()) {
				return;
			}

			T sourceElement = source.next();
			long otherElementId = other.next();

			while(idOf(sourceElement) < otherElementId) {
				if(!source.hasNext()) {
					return;
				}
				sourceElement = source.next();
			}
			while(idOf(sourceElement) > otherElementId) {
				if(!other.hasNext()) {
					return;
				}
				otherElementId = other.next();
			}
			if(idOf(sourceElement) == otherElementId) {
				current = sourceElement;
			}
		}

		@Override
		public T next() {
			if(!hasNext())
				throw new NoSuchElementException();
			final T next = current;
			current = null;
			return next;
		}

		@Override
		public Class<T> schemaClass() {
			return source.schemaClass();
		}
	}

	class Map<T extends Schema, R extends Schema> extends LedStreamOperation<T, R> {

		private static final int DEFAULT_ELEMENTS_PER_STACK = 1024;


		private final SchemaAllocator<R> allocator;
		private final BiConsumer<T, R> mapper;

		public Map(LedStream<T> source, SchemaAllocator<R> allocator, BiConsumer<T, R> mapper) {
			super(source);
			this.allocator = requireNonNull(allocator);
			this.mapper = requireNonNull(mapper);
		}

		public Map(LedStream<T> source, int rSize, Class<R> destinationClass, BiConsumer<T, R> mapper) {
			this(source, getDefaultAllocator(rSize, destinationClass), mapper);
		}

		@Override
		public int schemaSize() {
			return allocator.schemaSize();
		}

		@Override
		public boolean hasNext() {
			return source.hasNext();
		}

		@Override
		public R next() {
			final R newElement = allocator.calloc();
			mapper.accept(source.next(), newElement);
			return newElement;
		}

		@Override
		public Class<R> schemaClass() {
			return allocator.schemaClass();
		}

		private static <R extends Schema> SchemaAllocator<R> getDefaultAllocator(int rSize, Class<R> schemaClass) {
			return new StackListAllocator<>(DEFAULT_ELEMENTS_PER_STACK, rSize, schemaClass, StackAllocators::managedStackAllocator);
		}
	}

	interface Builder<T extends Schema> {

		Class<T> schemaClass();

		int schemaSize();

		Builder<T> append(Consumer<T> initializer);

		LedStream<T> build();
	}
}
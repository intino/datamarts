package systems.intino.datamarts.led.leds;

import io.intino.alexandria.logger.Logger;
import systems.intino.datamarts.led.LedLibraryConfig;
import systems.intino.datamarts.led.LedStream;
import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.SchemaFactory;
import systems.intino.datamarts.led.allocators.stack.StackAllocator;
import systems.intino.datamarts.led.allocators.stack.StackAllocators;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static systems.intino.datamarts.led.LedLibraryConfig.INPUT_LEDSTREAM_CONCURRENCY_ENABLED;
import static systems.intino.datamarts.led.util.memory.MemoryUtils.allocBuffer;

public class InputLedStream<T extends Schema> implements LedStream<T> {

    private static final int DEFAULT_BUFFER_SIZE = LedLibraryConfig.DEFAULT_BUFFER_SIZE.get();

    private final InputStream inputStream;
    private final int bufferSize;
    private final int schemaSize;
    private final SchemaFactory<T> factory;
    private final Iterator<T> iterator;
    private Runnable onClose;
    private final AtomicBoolean closed;
    private boolean concurrencyEnabled = INPUT_LEDSTREAM_CONCURRENCY_ENABLED.get();

    public InputLedStream(InputStream inputStream, SchemaFactory<T> factory, int schemaSize, int bufferSize) {
        this.inputStream = requireNonNull(inputStream);
        this.schemaSize = assertIsPositive(schemaSize);
        this.factory = factory;
        this.bufferSize = assertIsPositive(bufferSize);
        this.closed = new AtomicBoolean();
        this.iterator = stream().iterator();
    }

    private int assertIsPositive(int size) {
        if(size <= 0) throw new IllegalArgumentException("Size must be >= 0");
        return size;
    }

    public int bufferSize() {
        return bufferSize;
    }

    public boolean concurrencyEnabled() {
        return this.concurrencyEnabled;
    }

    public InputLedStream<T> concurrencyEnabled(boolean concurrencyEnabled) {
        this.concurrencyEnabled = concurrencyEnabled;
        return this;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        return iterator.next();
    }

    @Override
    public LedStream<T> onClose(Runnable onClose) {
        this.onClose = onClose;
        return this;
    }

    @Override
    public Class<T> schemaClass() {
        return factory.schemaClass();
    }

    @Override
    public int schemaSize() {
        return schemaSize;
    }

    private synchronized Stream<T> stream() {
        return Stream.generate(() -> read(inputStream))
                .takeWhile(inputBuffer -> checkInputBuffer(inputBuffer, inputStream))
                .flatMap(this::allocateAll);
    }

    private boolean checkInputBuffer(ByteBuffer inputBuffer, InputStream inputStream) {
        if (inputBuffer != null) return true;
        closeInputStream(inputStream);
        return false;
    }

    private synchronized void closeInputStream(InputStream inputStream) {
        try {
            inputStream.close();
        } catch (IOException e) {
            Logger.error(e);
        }
    }

    private Stream<T> allocateAll(ByteBuffer buffer) {
        StackAllocator<T> allocator = StackAllocators.managedStackAllocatorFromBuffer(schemaSize, buffer, factory.schemaClass());
        IntStream intStream = IntStream.range(0, buffer.remaining() / schemaSize);
        if(concurrencyEnabled)
            intStream = intStream.sorted().parallel();
        return intStream.mapToObj(index -> allocator.malloc());
    }

    private synchronized ByteBuffer read(InputStream inputStream) {
        try {
            if (inputStream == null || inputStream.available() <= 0) return null;
            byte[] inputBuffer = new byte[bufferSize * schemaSize];
            int bytesRead;
            bytesRead = inputStream.read(inputBuffer);
            if (bytesRead < 0) return null;
            ByteBuffer buffer = allocBuffer(bytesRead);
            buffer.put(inputBuffer, 0, bytesRead);
            buffer.clear();
            return buffer;
        } catch (Exception e) {
            Logger.error(e);
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        if (closed.get()) {
            return;
        }
        if (onClose != null) {
            onClose.run();
            onClose = null;
        }
        inputStream.close();
        closed.set(true);
    }

    public static class Builder<T extends Schema> {

        private InputStream inputStream;
        private int bufferSize = DEFAULT_BUFFER_SIZE;
        private int schemaSize = -1;
        private SchemaFactory<?> factory;
        private Runnable onClose;
        private boolean concurrencyEnabled = INPUT_LEDSTREAM_CONCURRENCY_ENABLED.get();

        public Builder<T> inputStream(InputStream inputStream) {
            this.inputStream = inputStream;
            return this;
        }

        public Builder<T> bufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        public Builder<T> schemaSize(int schemaSize) {
            this.schemaSize = schemaSize;
            return this;
        }

        public Builder<T> factory(SchemaFactory<?> factory) {
            this.factory = factory;
            return this;
        }

        public Builder<T> onClose(Runnable onClose) {
            this.onClose = onClose;
            return this;
        }

        public Builder<T> concurrencyEnabled(boolean concurrencyEnabled) {
            this.concurrencyEnabled = concurrencyEnabled;
            return this;
        }

        @SuppressWarnings("unchecked")
        public InputLedStream<T> build() {
            return (InputLedStream<T>) new InputLedStream<>(inputStream, factory, schemaSize, bufferSize)
                    .concurrencyEnabled(concurrencyEnabled)
                    .onClose(onClose);
        }
    }
}

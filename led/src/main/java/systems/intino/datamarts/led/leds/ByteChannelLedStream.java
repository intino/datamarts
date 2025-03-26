package systems.intino.datamarts.led.leds;

import io.intino.alexandria.logger.Logger;
import systems.intino.datamarts.led.LedStream;
import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.SchemaFactory;
import systems.intino.datamarts.led.allocators.stack.StackAllocator;
import systems.intino.datamarts.led.allocators.stack.StackAllocators;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.READ;
import static systems.intino.datamarts.led.LedLibraryConfig.DEFAULT_BUFFER_SIZE;
import static systems.intino.datamarts.led.LedLibraryConfig.INPUT_LEDSTREAM_CONCURRENCY_ENABLED;
import static systems.intino.datamarts.led.Schema.factoryOf;
import static systems.intino.datamarts.led.Schema.sizeOf;
import static systems.intino.datamarts.led.util.memory.MemoryUtils.allocBuffer;

public class ByteChannelLedStream<T extends Schema> implements LedStream<T> {

    private final FileChannel byteChannel;
    private final long fileSize;
    private final int bufferSize;
    private final int schemaSize;
    private final SchemaFactory<T> factory;
    private final Iterator<T> iterator;
    private Runnable onClose;
    private final AtomicBoolean closed;

    public ByteChannelLedStream(File file, Class<T> schemaClass) {
        this(file, factoryOf(schemaClass), sizeOf(schemaClass), DEFAULT_BUFFER_SIZE.get());
    }

    public ByteChannelLedStream(File file, Class<T> schemaClass, int bufferSize) {
        this(file, factoryOf(schemaClass), sizeOf(schemaClass), bufferSize);
    }

    public ByteChannelLedStream(File file, SchemaFactory<T> factory, int schemaSize) {
        this(file, factory, schemaSize, DEFAULT_BUFFER_SIZE.get());
    }

    public ByteChannelLedStream(File file, SchemaFactory<T> factory, int schemaSize, int bufferSize) {
        this.byteChannel = open(file);
        fileSize = getFileSize();
        this.schemaSize = schemaSize;
        this.factory = factory;
        this.bufferSize = bufferSize;
        closed = new AtomicBoolean();
        this.iterator = stream().iterator();
    }

    private long getFileSize() {
        try {
            return byteChannel.size();
        } catch (IOException e) {
            Logger.error(e);
            throw new RuntimeException(e);
        }
    }

    private FileChannel open(File file) {
        try {
            FileChannel byteChannel = FileChannel.open(file.toPath(), READ);
            byteChannel.position(0);
            return byteChannel;
        } catch (IOException e) {
            Logger.error(e);
            throw new RuntimeException(e);
        }
    }

    public int bufferSize() {
        return bufferSize;
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

    public synchronized Stream<T> stream() {
        return Stream.generate(() -> read(byteChannel))
                .takeWhile(inputBuffer -> checkInputBuffer(inputBuffer, byteChannel))
                .flatMap(this::allocateAll);
    }

    @Override
    public int schemaSize() {
        return schemaSize;
    }

    private boolean checkInputBuffer(ByteBuffer inputBuffer, FileChannel byteChannel) {
        if (inputBuffer != null) return true;
        closeByteChannel(byteChannel);
        return false;
    }

    private synchronized void closeByteChannel(FileChannel byteChannel) {
        try {
            byteChannel.close();
        } catch (IOException e) {
            Logger.error(e);
        }
    }

    private Stream<T> allocateAll(ByteBuffer buffer) {
        StackAllocator<T> allocator = StackAllocators.managedStackAllocatorFromBuffer(schemaSize, buffer, schemaClass());
        IntStream intStream = IntStream.range(0, buffer.remaining() / schemaSize);
        if(INPUT_LEDSTREAM_CONCURRENCY_ENABLED.get()) {
            intStream = intStream.sorted().parallel();
        }
        return intStream.mapToObj(index -> allocator.malloc());
    }

    private synchronized ByteBuffer read(FileChannel byteChannel) {
        try {
            if(byteChannel == null) {
                return null;
            }
            final long filePosition = byteChannel.position();
            if (!byteChannel.isOpen() || filePosition >= fileSize) {
                return null;
            }
            final int size = (int) Math.min(bufferSize, fileSize - filePosition) * schemaSize;
            ByteBuffer buffer = allocBuffer(size);
            int bytesRead = byteChannel.read(buffer);
            if (bytesRead <= 0) return null;
            buffer.position(0).limit(bytesRead);
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
        if(byteChannel != null) {
            byteChannel.close();
        }
        closed.set(true);
    }
}

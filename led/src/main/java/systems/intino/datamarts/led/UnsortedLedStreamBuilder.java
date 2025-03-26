package systems.intino.datamarts.led;

import io.intino.alexandria.logger.Logger;
import systems.intino.datamarts.led.allocators.SchemaFactory;
import systems.intino.datamarts.led.allocators.stack.StackAllocator;
import systems.intino.datamarts.led.allocators.stack.StackAllocators;
import systems.intino.datamarts.led.leds.InputLedStream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.WRITE;
import static systems.intino.datamarts.led.util.memory.MemoryUtils.allocBuffer;

public class UnsortedLedStreamBuilder<T extends Schema> implements LedStream.Builder<T>, AutoCloseable {

    private static final int DEFAULT_NUM_ELEMENTS_PER_BLOCK = 500_000;


    private final Class<T> schemaClass;
    private final int schemaSize;
    private final SchemaFactory<T> factory;
    private final Path tempLedFile;
    private final UUID serialUUID;
    private ByteBuffer buffer;
    private StackAllocator<T> allocator;
    private FileChannel fileChannel;
    private final AtomicLong numTransactions;
    private final boolean keepFileChannelOpen;
    private final AtomicBoolean closed;

    public UnsortedLedStreamBuilder(Class<T> schemaClass, File tempFile) {
        this(schemaClass, Schema.factoryOf(schemaClass),
                DEFAULT_NUM_ELEMENTS_PER_BLOCK, tempFile, true);
    }

    public UnsortedLedStreamBuilder(Class<T> schemaClass, File tempFile, boolean keepFileChannelOpen) {
        this(schemaClass, Schema.factoryOf(schemaClass),
                DEFAULT_NUM_ELEMENTS_PER_BLOCK, tempFile, keepFileChannelOpen);
    }

    public UnsortedLedStreamBuilder(Class<T> schemaClass, SchemaFactory<T> factory,
                                    int numElementsPerBlock, File tempFile) {
        this(schemaClass, factory, numElementsPerBlock, tempFile, true);
    }

    public UnsortedLedStreamBuilder(Class<T> schemaClass, SchemaFactory<T> factory,
                                    int numElementsPerBlock, File tempFile, boolean keepFileChannelOpen) {
        this.schemaClass = schemaClass;
        this.schemaSize = Schema.sizeOf(schemaClass);
        this.serialUUID = Schema.getSerialUUID(schemaClass);
        this.factory = factory;
        final File parentFile = tempFile.getParentFile();
        if(parentFile != null) parentFile.mkdirs();
        this.tempLedFile = tempFile.toPath();
        if(numElementsPerBlock % 2 != 0) {
            throw new IllegalArgumentException("NumElementsPerBlock must be even");
        }
        buffer = allocBuffer((long) numElementsPerBlock * schemaSize);
        this.allocator = StackAllocators.managedStackAllocatorFromBuffer(schemaSize, buffer, schemaClass);
        this.keepFileChannelOpen = keepFileChannelOpen;
        this.closed = new AtomicBoolean(false);
        this.numTransactions = new AtomicLong();
        setupFile();
    }

    private void setupFile() {
        try {
            if(!Files.exists(tempLedFile)) Files.createFile(tempLedFile);
            if(keepFileChannelOpen) fileChannel = openFileChannel();
            reserveHeader();
        } catch(Exception e) {
            Logger.error(e);
        }
    }

    public File tempLedFile() {
        return tempLedFile.toFile();
    }

    private FileChannel openFileChannel() throws IOException {
        return FileChannel.open(tempLedFile, WRITE, APPEND);
    }

    private void reserveHeader() throws IOException {
        if(!keepFileChannelOpen) fileChannel = openFileChannel();
        fileChannel.write(ByteBuffer.allocate(LedHeader.SIZE));
        if(!keepFileChannelOpen) fileChannel.close();
    }

    @Override
    public Class<T> schemaClass() {
        return schemaClass;
    }

    @Override
    public int schemaSize() {
        return schemaSize;
    }

    @Override
    public LedStream.Builder<T> append(Consumer<T> initializer) {
        if(isClosed()) {
            Logger.error("Trying to use a closed builder.");
            return this;
        }

        initializer.accept(allocator.calloc());

        if(allocator.remainingBytes() == 0) {
            writeCurrentBlockAndClear();
        }

        numTransactions.incrementAndGet();
        return this;
    }

    private synchronized void writeCurrentBlockAndClear() {
        try {
            if(!keepFileChannelOpen) {
                fileChannel = openFileChannel();
            }
            buffer.limit((int) allocator.stackPointer());
            while(buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }
            if(!keepFileChannelOpen) {
                fileChannel.close();
            }
            buffer.clear();
            allocator.clear();
        } catch (IOException e) {
            Logger.error(e);
            throw new RuntimeException(e);
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    public synchronized void flush() {
        if(isClosed()) {
            return;
        }
        writeCurrentBlockAndClear();
    }

    @Override
    public synchronized void close() {
        if(closed.compareAndSet(false, true)) {
            writeCurrentBlockAndClear();
            free();
            writeHeader();
        }
    }

    @Override
    public synchronized LedStream<T> build() {
        if(closed.get()) {
            Logger.warn("Trying to call build over a closed " + getClass().getSimpleName() + "...");
            return LedStream.empty(schemaClass);
        }
        close();
        return new InputLedStream.Builder<T>()
                .inputStream(getInputStream())
                .factory(factory)
                .schemaSize(schemaSize)
                .onClose(this::deleteTempFile)
                .build();
    }

    private void writeHeader() {
        LedHeader header = new LedHeader();
        header.elementCount(numTransactions.get());
        header.elementSize(schemaSize);
        header.uuid(serialUUID);
        try(RandomAccessFile file = new RandomAccessFile(tempLedFile.toFile(), "rw")) {
            LedHeader.write(header, file);
        } catch (IOException e) {
            Logger.error(e);
            throw new RuntimeException(e);
        }
    }

    private void deleteTempFile() {
        tempLedFile.toFile().delete();
        tempLedFile.toFile().deleteOnExit();
    }

    private void free() {
        try {
            allocator.free();
            allocator = null;
            buffer = null;
            fileChannel.close();
            fileChannel = null;
        } catch(Exception e) {
            Logger.error(e);
            throw new RuntimeException(e);
        }
    }

    private InputStream getInputStream() {
        try {
            final InputStream inputStream = Files.newInputStream(tempLedFile);
            inputStream.skip(LedHeader.SIZE);
            return inputStream;
        } catch (IOException e) {
            Logger.error(e);
            throw new RuntimeException(e);
        }
    }
}

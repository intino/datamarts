package systems.intino.datamarts.led;

import io.intino.alexandria.logger.Logger;
import org.xerial.snappy.SnappyInputStream;
import systems.intino.datamarts.led.allocators.SchemaFactory;
import systems.intino.datamarts.led.allocators.indexed.IndexedAllocator;
import systems.intino.datamarts.led.allocators.indexed.IndexedAllocatorFactory;
import systems.intino.datamarts.led.leds.ByteChannelLedStream;
import systems.intino.datamarts.led.leds.IndexedLed;
import systems.intino.datamarts.led.leds.InputLedStream;

import java.io.*;
import java.util.Objects;
import java.util.UUID;

import static systems.intino.datamarts.led.LedLibraryConfig.CHECK_SERIAL_ID;

public class LedReader {

	private final InputStream srcInputStream;
	private final File sourceFile;

	public LedReader(File file) {
		this.srcInputStream = inputStreamOf(file);
		this.sourceFile = file;
	}

	public LedReader(InputStream srcInputStream) {
		this.srcInputStream = srcInputStream;
		this.sourceFile = null;
	}

	public int size() {
		if(sourceFile == null) return (int) LedHeader.UNKNOWN_SIZE;
		try(RandomAccessFile raFile = new RandomAccessFile(sourceFile, "r")) {
			return (int) raFile.readLong();
		} catch (IOException e) {
			Logger.error(e);
		}
		return (int) LedHeader.UNKNOWN_SIZE;
	}

	public <T extends Schema> Led<T> readAll(Class<T> schemaClass) {
		return readAll(getDefaultAllocatorFactory(), schemaClass);
	}

	public <T extends Schema> Led<T> readAll(IndexedAllocatorFactory<T> allocatorFactory, Class<T> schemaClass) {
		try {
			if(srcInputStream.available() == 0) return Led.empty(schemaClass);
		} catch(Exception e) {
			Logger.error(e);
			return Led.empty(schemaClass);
		}
		LedHeader header = LedHeader.from(this.srcInputStream);
		if(CHECK_SERIAL_ID.get()) checkSerialUUID(header.uuid(), Schema.getSerialUUID(schemaClass));
		return readAllIntoMemory(allocatorFactory, schemaClass, header);
	}

	private <T extends Schema> Led<T> readAllIntoMemory(IndexedAllocatorFactory<T> allocatorFactory, Class<T> schemaClass, LedHeader header) {
		try(SnappyInputStream inputStream = new SnappyInputStream(this.srcInputStream)) {
			IndexedAllocator<T> allocator = allocatorFactory.create(inputStream, header.elementCount(), (int)header.elementSize(), schemaClass);
			return new IndexedLed<>(allocator);
		} catch (IOException e) {
			Logger.error(e);
		}
		return Led.empty(schemaClass);
	}

	public <T extends Schema> LedStream<T> read(Class<T> schemaClass) {
		try {
			if(srcInputStream.available() == 0) return LedStream.empty(schemaClass);
			LedHeader header = LedHeader.from(srcInputStream);
			checkSerialUUID(header.uuid(), Schema.getSerialUUID(schemaClass));
			return readAsStream(new SnappyInputStream(srcInputStream), schemaClass, (int)header.elementSize());
		} catch (IOException e) {
			Logger.error(e);
		}
		return LedStream.empty(schemaClass);
	}

	public <T extends Schema> LedStream<T> readUncompressed(int elementSize, Class<T> schemaClass) {
		try {
			if(srcInputStream.available() == 0) return LedStream.empty(schemaClass);
			return allocateUncompressed(Schema.factoryOf(schemaClass), elementSize);
		} catch (IOException e) {
			Logger.error(e);
		}
		return LedStream.empty(schemaClass);
	}

	private <T extends Schema> LedStream<T> allocateUncompressed(SchemaFactory<T> factory, int elementSize) {
		try {
			srcInputStream.close();
		} catch (IOException e) {
			Logger.error(e);
		}
		return new ByteChannelLedStream<>(sourceFile, factory, elementSize);
	}

	private <T extends Schema> LedStream<T> readAsStream(InputStream inputStream, Class<T> schemaClass, int schemaSize) {
		return new InputLedStream.Builder<T>()
				.inputStream(inputStream)
				.factory(Schema.factoryOf(schemaClass))
				.schemaSize(schemaSize)
				.build();
	}

	private static InputStream inputStreamOf(File file) {
		try {
			return new FileInputStream(file);
		} catch (FileNotFoundException e) {
			Logger.error("Failed to create FileInputStream for file " + file, e);
			return new ByteArrayInputStream(new byte[0]);
		}
	}

	private void checkSerialUUID(UUID srcUUID, UUID dstUUID) {
		if(srcUUID == null || dstUUID == null) return;
		if(!Objects.equals(srcUUID, dstUUID)) {
			throw new SchemaSerialUUIDMismatchException(srcUUID, dstUUID);
		}
	}

	private <T extends Schema> IndexedAllocatorFactory<T> getDefaultAllocatorFactory() {
		return (inputStream, elementCount, elementSize, schemaClass) -> {
			if(elementCount >= 0 && elementCount * elementSize < Integer.MAX_VALUE) {
				return IndexedAllocatorFactory.newManagedIndexedAllocator(inputStream, elementCount, elementSize, schemaClass);
			}
			return IndexedAllocatorFactory.newArrayAllocator(inputStream, elementCount, elementSize, schemaClass);
		};
	}

}
package systems.intino.datamarts.led;

import io.intino.alexandria.logger.Logger;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;

import static java.nio.file.StandardOpenOption.WRITE;
import static systems.intino.datamarts.led.util.memory.MemoryUtils.*;

public class LedWriter {

	private int bufferSize = LedLibraryConfig.DEFAULT_BUFFER_SIZE.get();
	private final OutputStream destOutputStream;
	private final File destinationFile;

	public LedWriter(File destOutputStream) {
		destOutputStream.getAbsoluteFile().getParentFile().mkdirs();
		this.destinationFile = destOutputStream;
		this.destOutputStream = outputStream(destOutputStream);
	}

	public LedWriter(OutputStream destOutputStream) {
		this.destOutputStream = destOutputStream;
		destinationFile = null;
	}

	public int bufferSize() {
		return bufferSize;
	}

	public LedWriter bufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	private FileOutputStream outputStream(File destination) {
		try {
			return new FileOutputStream(destination);
		} catch (FileNotFoundException e) {
			Logger.error(e);
			return null;
		}
	}

	public void write(Led<? extends Schema> led) {
		serialize(led);
	}

	public void write(LedStream<? extends Schema> led) {
		serialize(led);
	}

	public <T extends Schema> void writeUncompressed(LedStream<T> ledStream) {
		if(destinationFile != null) {
			fastSerializeUncompressed(ledStream);
		} else {
			serializeUncompressed(ledStream);
		}
	}

	private <T extends Schema> void fastSerializeUncompressed(LedStream<T> ledStream) {
		try (FileChannel fileChannel = FileChannel.open(destinationFile.toPath(), WRITE)) {
			final int schemaSize = ledStream.schemaSize();
			ByteBuffer outputBuffer = allocBuffer((long) bufferSize * schemaSize);
			final long destPtr = addressOf(outputBuffer);
			int offset = 0;
			while (ledStream.hasNext()) {
				Schema schema = ledStream.next();
				memcpy(schema.address() + schema.baseOffset(), destPtr + offset, schemaSize);
				offset += schemaSize;
				if (offset == outputBuffer.capacity()) {
					fileChannel.write(outputBuffer);
					outputBuffer.clear();
					offset = 0;
				}
			}
			if (offset > 0) {
				outputBuffer.limit(offset);
				fileChannel.write(outputBuffer);
				outputBuffer.clear();
			}
			destOutputStream.close();
			ledStream.close();
		} catch (Exception e) {
			Logger.error(e);
		}
	}

	private <T extends Schema> void serializeUncompressed(LedStream<T> ledStream) {
		try (OutputStream outputStream = this.destOutputStream) {
			final int schemaSize = ledStream.schemaSize();
			final byte[] outputBuffer = new byte[bufferSize * schemaSize];
			int offset = 0;
			while (ledStream.hasNext()) {
				Schema schema = ledStream.next();
				memcpy(schema.address(), schema.baseOffset(), outputBuffer, offset, schemaSize);
				offset += schemaSize;
				if (offset == outputBuffer.length) {
					writeToOutputStream(outputStream, outputBuffer);
					offset = 0;
				}
			}
			if (offset > 0) {
				writeToOutputStream(outputStream, outputBuffer, 0, offset);
			}
			ledStream.close();
		} catch (Exception e) {
			Logger.error(e);
		}
	}

	private void serialize(Led<? extends Schema> led) {
		if (led.size() == 0) return;
		final long size = led.size();
		final int schemaSize = led.schemaSize();
		final int numBatches = (int) Math.ceil(led.size() / (float) bufferSize);
		try (OutputStream originalOutputStream = this.destOutputStream) {
			LedHeader header = new LedHeader();
			header.elementCount(size).elementSize(schemaSize).uuid(led.serialUUID());
			originalOutputStream.write(header.toByteArray());
			writeLed(led, schemaSize, numBatches, originalOutputStream);
		} catch (Exception e) {
			Logger.error(e);
		}
	}

	private void writeLed(Led<? extends Schema> led, int schemaSize, int numBatches, OutputStream fos) throws IOException {
		try (SnappyOutputStream outputStream = new SnappyOutputStream(fos)) {
			for (int i = 0; i < numBatches; i++) {
				final int start = i * bufferSize;
				final int numElements = (int) Math.min(bufferSize, led.size() - start);
				byte[] outputBuffer = new byte[numElements * schemaSize];
				for (int j = 0; j < numElements; j++) {
					Schema src = led.schema(j + start);
					final long offset = (long) j * schemaSize;
					memcpy(src.address(), src.baseOffset(), outputBuffer, offset, schemaSize);
				}
				writeToOutputStream(outputStream, outputBuffer);
			}
		}
	}

	private void serialize(LedStream<? extends Schema> ledStream) {
		long elementCount = 0;
		try (OutputStream outputStream = this.destOutputStream) {
			reserveHeader(ledStream, outputStream);
			elementCount = writeLedStream(ledStream, elementCount, outputStream);
			ledStream.close();
		} catch (Exception e) {
			Logger.error(e);
		}
		if(destinationFile != null)
			overrideHeader(elementCount, ledStream.schemaSize(), ledStream.serialUUID());
	}

	private long writeLedStream(LedStream<? extends Schema> ledStream, long elementCount, OutputStream outputStream) throws IOException {
		try (SnappyOutputStream snappyOutputStream = new SnappyOutputStream(outputStream)) {
			final int schemaSize = ledStream.schemaSize();
			final byte[] outputBuffer = new byte[bufferSize * schemaSize];
			int offset = 0;
			while (ledStream.hasNext()) {
				Schema schema = ledStream.next();
				memcpy(schema.address(), schema.baseOffset(), outputBuffer, offset, schemaSize);
				offset += schemaSize;
				if (offset == outputBuffer.length) {
					writeToOutputStream(snappyOutputStream, outputBuffer);
					offset = 0;
				}
				++elementCount;
			}
			if (offset > 0)
				writeToOutputStream(snappyOutputStream, outputBuffer, 0, offset);
		}
		return elementCount;
	}

	private void reserveHeader(LedStream<? extends Schema> ledStream, OutputStream fos) throws IOException {
		LedHeader header = new LedHeader();
		header.elementCount(LedHeader.UNKNOWN_SIZE).elementSize(ledStream.schemaSize()).uuid(ledStream.serialUUID());
		fos.write(header.toByteArray());
	}

	private void overrideHeader(long elementCount, int schemaSize, UUID uuid) {
		try(RandomAccessFile raFile = new RandomAccessFile(destinationFile, "rw")) {
			raFile.writeLong(elementCount);
			raFile.writeLong(schemaSize);
			raFile.writeLong(uuid.getMostSignificantBits());
			raFile.writeLong(uuid.getLeastSignificantBits());
		} catch (IOException e) {
			Logger.error(e);
		}
	}

	private void writeToOutputStream(OutputStream outputStream, byte[] outputBuffer) {
		writeToOutputStream(outputStream, outputBuffer, 0, outputBuffer.length);
	}

	private void writeToOutputStream(OutputStream outputStream, byte[] outputBuffer, int offset, int size) {
		try {
			outputStream.write(outputBuffer, offset, size);
		} catch (IOException e) {
			Logger.error(e);
		}
	}
}

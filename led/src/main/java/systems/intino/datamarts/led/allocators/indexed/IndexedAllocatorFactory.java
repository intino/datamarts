package systems.intino.datamarts.led.allocators.indexed;

import io.intino.alexandria.logger.Logger;
import systems.intino.datamarts.led.LedLibraryConfig;
import systems.intino.datamarts.led.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static systems.intino.datamarts.led.util.memory.MemoryUtils.allocBuffer;
import static systems.intino.datamarts.led.util.memory.MemoryUtils.memcpy;

public interface IndexedAllocatorFactory<T extends Schema> {

    static <S extends Schema> ManagedIndexedAllocator<S> newManagedIndexedAllocator(InputStream inputStream,
                                                                                    long elementCount,
                                                                                    int elementSize,
                                                                                    Class<S> schemaClass) {

        try {
            if (elementCount < 0) {
                throw new IllegalArgumentException("Element count cannot be < 0");
            }
            if (elementCount * elementSize >= Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Buffer size too large for ManagedIndexedAllocator");
            }

            ByteBuffer buffer = allocBuffer(elementCount * elementSize);

            byte[] inputBuffer = new byte[LedLibraryConfig.DEFAULT_BUFFER_SIZE.get()];

            int bytesRead;
            while((bytesRead = inputStream.read(inputBuffer)) > 0) {
                buffer.put(inputBuffer, 0, bytesRead);
            }

            buffer.clear();

            return new ManagedIndexedAllocator<>(buffer, 0, buffer.capacity(), elementSize, schemaClass);

        } catch (IOException e) {
            Logger.error(e);
        }
        return null;
    }

    static <S extends Schema> IndexedAllocator<S> newArrayAllocator(InputStream inputStream,
                                                                    long elementCount,
                                                                    int elementSize,
                                                                    Class<S> schemaClass) {

        try {

            List<ByteBuffer> buffers = new ArrayList<>();

            byte[] inputBuffer = new byte[LedLibraryConfig.DEFAULT_BUFFER_SIZE.get() * elementSize];

            int bytesRead;

            while ((bytesRead = inputStream.read(inputBuffer)) > 0) {
                ByteBuffer buffer = allocBuffer(bytesRead);
                memcpy(inputBuffer, 0, buffer, 0, bytesRead);
                buffers.add(buffer);
            }

            return new ArrayAllocator<>(buffers, elementSize, schemaClass);

        } catch (IOException e) {
            Logger.error(e);
        }
        return null;
    }


    IndexedAllocator<T> create(InputStream inputStream, long elementCount, int elementSize, Class<T> schemaClass) throws IOException;

}

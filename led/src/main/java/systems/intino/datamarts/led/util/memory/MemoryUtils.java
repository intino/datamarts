package systems.intino.datamarts.led.util.memory;

import io.intino.alexandria.logger.Logger;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import static systems.intino.datamarts.led.LedLibraryConfig.*;

public final class MemoryUtils {
	public static final long NULL = 0L;

	private static final Unsafe UNSAFE;
	private static final Class<? extends ByteBuffer> DIRECT_BUFFER_CLASS;
	private static final long BUFFER_ADDRESS_OFFSET;
	private static final long BUFFER_POSITION_OFFSET;
	private static final long BUFFER_MARK_OFFSET;
	private static final long BUFFER_CAPACITY_OFFSET;
	private static final long BUFFER_LIMIT_OFFSET;

	private static final Variable<NativeMemoryTracker> MEMORY_TRACKER = new Variable<>();

	public static boolean useMemoryTracker() {
		if(USE_MEMORY_TRACKER.isEmpty()) {
			USE_MEMORY_TRACKER.set(false);
		}
		return USE_MEMORY_TRACKER.get();
	}

	public static NativeMemoryTracker getMemoryTracker() {
		if(MEMORY_TRACKER.isEmpty()) {
			if(useMemoryTracker()) {
				MEMORY_TRACKER.set(new NativeMemoryTrackerImpl());
			} else {
				MEMORY_TRACKER.set(new DummyNativeMemoryTracker());
			}
		}
		return MEMORY_TRACKER.get();
	}

	public static MappedByteBuffer map(FileChannel fileChannel, FileChannel.MapMode mode, long baseOffset, long size) {
		try {
			MappedByteBuffer mappedByteBuffer = fileChannel.map(mode, baseOffset, size);
			mappedByteBuffer.order(BYTE_ORDER.get());
			return mappedByteBuffer;
		} catch(Exception e) {
			Logger.error(e);
			throw new RuntimeException(e);
		}
	}

	public static ByteBuffer allocBuffer(long size) {
		return allocBuffer(size, BYTE_ORDER.get());
	}

	public static synchronized ByteBuffer allocBuffer(long size, ByteOrder order) {
		if (size < 0) {
			throw new IllegalArgumentException("Size is negative or too large");
		}
		if (size > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Size " + size + " too large for ByteBuffer. " +
					"Do smaller allocations or use unmanaged memory.");
		}
		if(!BEFORE_ALLOCATION_CALLBACK.isEmpty()) {
			BEFORE_ALLOCATION_CALLBACK.get().accept(size);
		}
		ByteBuffer buffer;
		try {
			buffer = ByteBuffer.allocateDirect((int) size);
			buffer.order(order);
			if(useMemoryTracker()) {
				track(buffer, size, caller());
			}
		} catch(OutOfMemoryError e) {
			Logger.error("Failed to allocate buffer of size " + size + "(int=" + size + "): " + e.getMessage(), e);
			throw new OutOfMemoryError(e.getMessage());
		}
		return buffer;
	}

	private static void track(ByteBuffer buffer, long size, StackTraceElement[] stackTrace) {
		NativeMemoryTrackerImpl nativeMemoryTracker = (NativeMemoryTrackerImpl) getMemoryTracker();
		AllocationInfo allocationInfo = new AllocationInfo(buffer, size, stackTrace);
		nativeMemoryTracker.track(allocationInfo);
		if(!ALLOCATION_CALLBACK.isEmpty()) {
			ALLOCATION_CALLBACK.get().accept(allocationInfo);
		}
	}

	private static StackTraceElement[] caller() {
		StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
		if(stackTrace.length <= 2) {
			return new StackTraceElement[0];
		}
		int start = 2;
		for(int i = start;i < stackTrace.length;i++) {
			if(!stackTrace[i].getClassName().endsWith(MemoryUtils.class.getName())) {
				start = i;
				break;
			}
		}
		return Arrays.copyOfRange(stackTrace, start, stackTrace.length);
	}

	public static long addressOf(Buffer buffer) {
		if (!buffer.isDirect()) {
			throw new IllegalArgumentException("Buffer is not direct");
		}
		return UNSAFE.getLong(buffer, BUFFER_ADDRESS_OFFSET);
	}

	public static long malloc(long bytes) {
		return UNSAFE.allocateMemory(bytes);
	}

	public static long calloc(long bytes) {
		final long ptr = malloc(bytes);
		memset(ptr, bytes, 0);
		return ptr;
	}

	public static long realloc(long ptr, long bytes) {
		return UNSAFE.reallocateMemory(ptr, bytes);
	}

	public static void memset(long ptr, long bytes, int value) {
		UNSAFE.setMemory(ptr, bytes, (byte) (value & 0xFF));
	}

	public static void memcpy(long src, long dest, long bytes) {
		UNSAFE.copyMemory(src, dest, bytes);
	}

	public static void memcpy(ByteBuffer src, long srcOffset, byte[] dest, long destOffset, long bytes) {
		memcpy(addressOf(src), srcOffset, dest, destOffset, bytes);
	}

	public static void memcpy(long srcAddress, long srcOffset, byte[] dest, long destOffset, long bytes) {
		UNSAFE.copyMemory(null, srcAddress + srcOffset, dest, arrayBaseOffset(dest.getClass()) + destOffset, bytes);
	}

	public static void memcpy(byte[] src, long srcOffset, ByteBuffer dest, long destOffset, long bytes) {
		memcpy(src, srcOffset, addressOf(dest), destOffset, bytes);
	}

	public static void memcpy(byte[] src, long srcOffset, long destAddress, long destOffset, long bytes) {
		UNSAFE.copyMemory(src, arrayBaseOffset(src.getClass()) + srcOffset, null, destAddress + destOffset, bytes);
	}

	public static void free(long ptr) {
		if(ptr == NULL) {
			return;
		}
		if(useMemoryTracker()) {
			if(!FREE_CALLBACK.isEmpty()) {
				AllocationInfo allocationInfo = getMemoryTracker().find(ptr);
				if(allocationInfo != null) {
					FREE_CALLBACK.get().accept(allocationInfo);
				}
			}
		}
		UNSAFE.freeMemory(ptr);
	}

	public static void free(ByteBuffer buffer) {
		if(buffer == null) {
			return;
		}
		if(useMemoryTracker()) {
			if(!FREE_CALLBACK.isEmpty()) {
				AllocationInfo allocationInfo = getMemoryTracker().find(addressOf(buffer));
				if(allocationInfo != null) {
					FREE_CALLBACK.get().accept(allocationInfo);
				}
			}
		}
		UNSAFE.invokeCleaner(buffer);
	}

	public static byte getByte(long ptr, long offset) {
		return UNSAFE.getByte(ptr + offset);
	}

	public static void setByte(long ptr, long offset, int value) {
		UNSAFE.putByte(ptr + offset, (byte) (value & 0xFF));
	}

	public static short getShort(long ptr, long offset) {
		return UNSAFE.getShort(ptr + offset);
	}

	public static void setShort(long ptr, long offset, int value) {
		UNSAFE.putShort(ptr + offset, (short) (value & 0xFFFF));
	}

	public static char getChar(long ptr, long offset) {
		return UNSAFE.getChar(ptr + offset);
	}

	public static void setChar(long ptr, long offset, char value) {
		UNSAFE.putChar(ptr + offset, value);
	}

	public static int getInt(long ptr, long offset) {
		return UNSAFE.getInt(ptr + offset);
	}

	public static void setInt(long ptr, long offset, int value) {
		UNSAFE.putInt(ptr + offset, value);
	}

	public static long getLong(long ptr, long offset) {
		return UNSAFE.getLong(ptr + offset);
	}

	public static void setLong(long ptr, long offset, long value) {
		UNSAFE.putLong(ptr + offset, value);
	}

	public static float getFloat(long ptr, long offset) {
		return UNSAFE.getFloat(ptr + offset);
	}

	public static void setFloat(long ptr, long offset, float value) {
		UNSAFE.putFloat(ptr + offset, value);
	}

	public static double getDouble(long ptr, long offset) {
		return UNSAFE.getDouble(ptr + offset);
	}

	public static void setDouble(long ptr, long offset, double value) {
		UNSAFE.putDouble(ptr + offset, value);
	}

	public static long arrayBaseOffset(Class<?> arrayClass) {
		return UNSAFE.arrayBaseOffset(arrayClass);
	}

	static {
		Unsafe unsafe = null;
		try {
			final Field field = Unsafe.class.getDeclaredField("theUnsafe");
			field.setAccessible(true);
			unsafe = (Unsafe) field.get(null);
		} catch (Exception e) {
			Logger.error(e);
		}
		UNSAFE = unsafe;
	}

	static {
		DIRECT_BUFFER_CLASS = ByteBuffer.allocateDirect(0).getClass();
	}

	static {
		long offset = 0;
		try {
			Field field = Buffer.class.getDeclaredField("address");
			offset = UNSAFE.objectFieldOffset(field);
		} catch (Exception e) {
			Logger.error(e);
		}
		BUFFER_ADDRESS_OFFSET = offset;
	}

	static {
		long offset = 0;
		try {
			Field field = Buffer.class.getDeclaredField("mark");
			offset = UNSAFE.objectFieldOffset(field);
		} catch (Exception e) {
			Logger.error(e);
		}
		BUFFER_MARK_OFFSET = offset;
	}

	static {
		long offset = 0;
		try {
			Field field = Buffer.class.getDeclaredField("position");
			offset = UNSAFE.objectFieldOffset(field);
		} catch (Exception e) {
			Logger.error(e);
		}
		BUFFER_POSITION_OFFSET = offset;
	}

	static {
		long offset = 0;
		try {
			Field field = Buffer.class.getDeclaredField("capacity");
			offset = UNSAFE.objectFieldOffset(field);
		} catch (Exception e) {
			Logger.error(e);
		}
		BUFFER_CAPACITY_OFFSET = offset;
	}

	static {
		long offset = 0;
		try {
			Field field = Buffer.class.getDeclaredField("limit");
			offset = UNSAFE.objectFieldOffset(field);
		} catch (Exception e) {
			Logger.error(e);
		}
		BUFFER_LIMIT_OFFSET = offset;
	}

	private MemoryUtils() {
	}

}

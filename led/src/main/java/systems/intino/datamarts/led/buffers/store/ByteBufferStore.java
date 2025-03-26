package systems.intino.datamarts.led.buffers.store;


import systems.intino.datamarts.led.util.memory.MemoryAddress;
import systems.intino.datamarts.led.util.memory.ModifiableMemoryAddress;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static java.util.Objects.requireNonNull;
import static systems.intino.datamarts.led.util.memory.MemoryUtils.allocBuffer;
import static systems.intino.datamarts.led.util.memory.MemoryUtils.memset;

public class ByteBufferStore implements ByteStore {

	private final ByteBuffer buffer;
	private final MemoryAddress address;
	private final int baseOffset;
	private final int size;

	public ByteBufferStore(int size) {
		ByteBuffer buffer = allocBuffer(size);
		this.buffer = buffer;
		this.address = ModifiableMemoryAddress.of(buffer);
		this.baseOffset = 0;
		this.size = size;
	}

	public ByteBufferStore(ByteBuffer buffer, MemoryAddress address, int baseOffset, int size) {
		this.buffer = requireNonNull(buffer);
		if (!buffer.isDirect()) throw new IllegalArgumentException("Buffer is not direct");
		this.address = address;
		if (Integer.toUnsignedLong(baseOffset) != baseOffset)
			throw new IllegalArgumentException("Base offset is negative or is too large for ByteBufferStore. Use " + NativePointerStore.class.getSimpleName() + " instead");
		if (Integer.toUnsignedLong(size) != size)
			throw new IllegalArgumentException("Size is negative or is too large for ByteBufferStore. Use " + NativePointerStore.class.getSimpleName() + " instead");
		this.baseOffset = baseOffset;
		this.size = size;
	}

	@Override
	public long address() {
		return address.get();
	}

	@Override
	public ByteOrder order() {
		return buffer.order();
	}

	@Override
	public ByteBuffer storeImpl() {
		return buffer;
	}

	@Override
	public long baseOffset() {
		return baseOffset;
	}

	@Override
	public long byteSize() {
		return size;
	}

	@Override
	public byte getByte(int byteIndex) {
		return buffer.get(byteIndex + baseOffset);
	}

	@Override
	public void setByte(int byteIndex, byte value) {
		buffer.put(byteIndex + baseOffset, value);
	}

	@Override
	public short getShort(int byteIndex) {
		return buffer.getShort(byteIndex + baseOffset);
	}

	@Override
	public void setShort(int byteIndex, short value) {
		buffer.putShort(byteIndex + baseOffset, value);
	}

	@Override
	public char getChar(int byteIndex) {
		return buffer.getChar(byteIndex + baseOffset);
	}

	@Override
	public void setChar(int byteIndex, char value) {
		buffer.putChar(byteIndex + baseOffset, value);
	}

	@Override
	public int getInt(int byteIndex) {
		return buffer.getInt(byteIndex + baseOffset);
	}

	@Override
	public void setInt(int byteIndex, int value) {
		buffer.putInt(byteIndex + baseOffset, value);
	}

	@Override
	public long getLong(int byteIndex) {
		return buffer.getLong(byteIndex + baseOffset);
	}

	@Override
	public void setLong(int byteIndex, long value) {
		buffer.putLong(byteIndex + baseOffset, value);
	}

	@Override
	public float getFloat(int byteIndex) {
		return buffer.getFloat(byteIndex + baseOffset);
	}

	@Override
	public void setFloat(int byteIndex, float value) {
		buffer.putFloat(byteIndex + baseOffset, value);
	}

	@Override
	public double getDouble(int byteIndex) {
		return buffer.getDouble(byteIndex + baseOffset);
	}

	@Override
	public void setDouble(int byteIndex, double value) {
		buffer.putDouble(byteIndex + baseOffset, value);
	}

	@Override
	public void clear() {
		memset(address() + baseOffset, size, 0);
	}

	@Override
	public ByteStore slice(long offset, long size) {
		if (offset + baseOffset > Integer.MAX_VALUE)
			throw new IllegalArgumentException("Offset too large for ByteBufferStore");
		if (size > Integer.MAX_VALUE) throw new IllegalArgumentException("Size too large for ByteBufferStore");
		return new ByteBufferStore(buffer, address, baseOffset + (int) offset, (int) size);
	}
}
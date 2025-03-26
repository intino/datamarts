package systems.intino.datamarts.led.buffers.store;

import systems.intino.datamarts.led.util.memory.MemoryAddress;
import systems.intino.datamarts.led.util.memory.MemoryUtils;

import java.nio.ByteOrder;

import static systems.intino.datamarts.led.util.memory.MemoryUtils.memset;

public class NativePointerStore implements ByteStore {

	private final MemoryAddress address;
	private final long baseOffset;
	private final long size;

	public NativePointerStore(MemoryAddress address, long baseOffset, long size) {
		this.address = address;
		this.size = size;
		if (size < 0) {
			throw new IllegalArgumentException("Size is negative or is too large");
		}
		this.baseOffset = baseOffset;
		if (baseOffset < 0) {
			throw new IllegalArgumentException("Base offset is negative or is too large");
		}
	}

	@Override
	public long address() {
		return address.get();
	}

	@Override
	public long byteSize() {
		return size;
	}

	@Override
	public ByteOrder order() { // TODO
		return ByteOrder.nativeOrder();
	}

	@Override
	public Long storeImpl() {
		return address.get();
	}

	@Override
	public long baseOffset() {
		return baseOffset;
	}

	@Override
	public byte getByte(int byteIndex) {
		return MemoryUtils.getByte(address.get(), byteIndex + baseOffset);
	}

	@Override
	public void setByte(int byteIndex, byte value) {
		MemoryUtils.setByte(address.get(), byteIndex + baseOffset, value);
	}

	@Override
	public short getShort(int byteIndex) {
		return MemoryUtils.getShort(address.get(), byteIndex + baseOffset);
	}

	@Override
	public void setShort(int byteIndex, short value) {
		MemoryUtils.setShort(address.get(), byteIndex + baseOffset, value);
	}

	@Override
	public char getChar(int byteIndex) {
		return MemoryUtils.getChar(address.get(), byteIndex + baseOffset);
	}

	@Override
	public void setChar(int byteIndex, char value) {
		MemoryUtils.setChar(address.get(), byteIndex + baseOffset, value);
	}

	@Override
	public int getInt(int byteIndex) {
		return MemoryUtils.getInt(address.get(), byteIndex + baseOffset);
	}

	@Override
	public void setInt(int byteIndex, int value) {
		MemoryUtils.setInt(address.get(), byteIndex + baseOffset, value);
	}

	@Override
	public long getLong(int byteIndex) {
		return MemoryUtils.getLong(address.get(), byteIndex + baseOffset);
	}

	@Override
	public void setLong(int byteIndex, long value) {
		MemoryUtils.setLong(address.get(), byteIndex + baseOffset, value);
	}

	@Override
	public float getFloat(int byteIndex) {
		return MemoryUtils.getFloat(address.get(), byteIndex + baseOffset);
	}

	@Override
	public void setFloat(int byteIndex, float value) {
		MemoryUtils.setFloat(address.get(), byteIndex + baseOffset, value);
	}

	@Override
	public double getDouble(int byteIndex) {
		return MemoryUtils.getDouble(address.get(), byteIndex + baseOffset);
	}

	@Override
	public void setDouble(int byteIndex, double value) {
		MemoryUtils.setDouble(address.get(), byteIndex + baseOffset, value);
	}

	@Override
	public void clear() {
		memset(address() + baseOffset, size, 0);
	}

	@Override
	public ByteStore slice(long baseOffset, long size) {
		return new NativePointerStore(address, this.baseOffset + baseOffset, size);
	}

}

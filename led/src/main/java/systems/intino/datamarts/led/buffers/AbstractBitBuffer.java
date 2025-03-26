package systems.intino.datamarts.led.buffers;

import systems.intino.datamarts.led.buffers.store.ByteStore;
import systems.intino.datamarts.led.buffers.store.ReadOnlyByteStore;
import systems.intino.datamarts.led.util.BitUtils;

import static java.util.Objects.requireNonNull;
import static systems.intino.datamarts.led.util.BitUtils.*;

public abstract class AbstractBitBuffer implements BitBuffer {

	private ByteStore store;

	public AbstractBitBuffer(ByteStore store) {
		this.store = requireNonNull(store);
	}

	@Override
	public final long address() {
		return store.address();
	}

	@Override
	public void invalidate() {
		if (!isReadOnly()) {
			store = new ReadOnlyByteStore(store);
		}
	}

	@Override
	public long byteSize() {
		return store.byteSize();
	}

	@Override
	public final long bitCount() {
		return store.bitCount();
	}

	@Override
	public long baseOffset() {
		return store.baseOffset();
	}

	@Override
	public long endOffset() {
		return baseOffset() + byteSize();
	}

	// ================================= BOOLEAN =================================

	@Override
	public boolean getBoolean(int bitIndex) {
		final short value = getByteNBits(bitIndex, 1);
		return value != 0;
	}

	@Override
	public void setBoolean(int bitIndex, boolean value) {
		setByteNBits(bitIndex, 1, (byte) (value ? 1 : 0));
	}

	// ================================= BYTE =================================

	@Override
	public byte getByteNBits(int bitIndex, int bitCount) {
		return (byte) extendSign(getNBits(bitIndex, bitCount), bitCount);
	}

	@Override
	public void setByteNBits(int bitIndex, int bitCount, byte value) {
		setNBits(value, bitIndex, bitCount);
	}

	@Override
	public short getUByteNBits(int bitIndex, int bitCount) {
		return (short) Byte.toUnsignedInt((byte) getNBits(bitIndex, bitCount));
	}

	@Override
	public void setUByteNBits(int bitIndex, int bitCount, short value) {
		setNBits(value & 0xFF, bitIndex, bitCount);
	}

	@Override
	public byte getAlignedByte(int bitIndex) {
		return (byte) extendSign((store.getByte(byteIndex(bitIndex)) & 0xFF), Byte.SIZE);
	}

	@Override
	public void setAlignedByte(int bitIndex, byte value) {
		store.setByte(byteIndex(bitIndex), value);
	}

	@Override
	public short getAlignedUByte(int bitIndex) {
		return (short) (store.getByte(byteIndex(bitIndex)) & 0xFF);
	}

	@Override
	public void setAlignedUByte(int bitIndex, short value) {
		store.setByte(byteIndex(bitIndex), (byte) (value & 0xFF));
	}

	// ================================= SHORT =================================

	@Override
	public short getShortNBits(int bitIndex, int bitCount) {
		final long x = extendSign(getNBits(bitIndex, bitCount), bitCount);
		return (short) x;
	}

	@Override
	public void setShortNBits(int bitIndex, int bitCount, short value) {
		setNBits(value, bitIndex, bitCount);
	}

	@Override
	public int getUShortNBits(int bitIndex, int bitCount) {
		return Short.toUnsignedInt((short) getNBits(bitIndex, bitCount));
	}

	@Override
	public void setUShortNBits(int bitIndex, int bitCount, int value) {
		setNBits(value & 0xFFFF, bitIndex, bitCount);
	}

	@Override
	public short getAlignedShort(int bitIndex) {
		return (short) extendSign(store.getShort(byteIndex(bitIndex)), Short.SIZE);
	}

	@Override
	public void setAlignedShort(int bitIndex, short value) {
		store.setShort(byteIndex(bitIndex), value);
	}

	@Override
	public int getAlignedUShort(int bitIndex) {
		return Short.toUnsignedInt(store.getShort(byteIndex(bitIndex)));
	}

	@Override
	public void setAlignedUShort(int bitIndex, int value) {
		store.setShort(byteIndex(bitIndex), (short) (value & 0xFFFF));
	}

	// ================================= INT =================================

	@Override
	public int getIntegerNBits(int bitIndex, int bitCount) {
		return (int) extendSign(getNBits(bitIndex, bitCount), bitCount);
	}

	@Override
	public void setIntegerNBits(int bitIndex, int bitCount, int value) {
		setNBits(value, bitIndex, bitCount);
	}

	@Override
	public long getUIntegerNBits(int bitIndex, int bitCount) {
		return getNBits(bitIndex, bitCount) & 0xFFFFFFFFL;
	}

	@Override
	public void setUIntegerNBits(int bitIndex, int bitCount, long value) {
		setNBits(value & 0xFFFFFFFFL, bitIndex, bitCount);
	}

	@Override
	public int getAlignedInteger(int bitIndex) {
		return (int) extendSign(store.getInt(byteIndex(bitIndex)), Integer.SIZE);
	}

	@Override
	public void setAlignedInteger(int bitIndex, int value) {
		store.setInt(byteIndex(bitIndex), value);
	}

	@Override
	public long getAlignedUInteger(int bitIndex) {
		return store.getInt(byteIndex(bitIndex)) & 0xFFFFFFFFL;
	}

	@Override
	public void setAlignedUInteger(int bitIndex, long value) {
		store.setInt(byteIndex(bitIndex), (int) (value & 0xFFFFFFFFL));
	}

	// ================================= LONG =================================

	@Override
	public long getLongNBits(int bitIndex, int bitCount) {
		return extendSign(getNBits(bitIndex, bitCount), bitCount);
	}

	@Override
	public void setLongNBits(int bitIndex, int bitCount, long value) {
		setNBits(value, bitIndex, bitCount);
	}

	@Override
	public long getULongNBits(int bitIndex, int bitCount) {
		if(bitCount == Long.SIZE) throw new UnsupportedOperationException("Unsigned long cannot have " + bitCount + " bits");
		return getNBits(bitIndex, bitCount);
	}

	@Override
	public void setULongNBits(int bitIndex, int bitCount, long value) {
		if(bitCount == Long.SIZE) throw new UnsupportedOperationException("Unsigned long cannot have " + bitCount + " bits");
		setNBits(value, bitIndex, bitCount);
	}

	@Override
	public long getAlignedLong(int bitIndex) {
		return extendSign(store.getLong(byteIndex(bitIndex)), Long.SIZE);
	}

	@Override
	public void setAlignedLong(int bitIndex, long value) {
		store.setLong(byteIndex(bitIndex), value);
	}

	@Override
	public long getAlignedULong(int bitIndex) {
		return store.getLong(byteIndex(bitIndex)) & 0x7FFFFFFFFFFFFFFFL;
	}

	@Override
	public void setAlignedULong(int bitIndex, long value) {
		store.setLong(byteIndex(bitIndex), value  & 0x7FFFFFFFFFFFFFFFL);
	}

	// ================================= FLOAT =================================

	@Override
	public float getReal32Bits(int bitIndex) {
		int byteIndex = byteIndex(bitIndex);
		final int numBytes = getMinimumBytesFor(bitIndex, Float.SIZE);
		final int additionalBytes = getAdditionalBytes(byteSize(), byteIndex, numBytes);
		byteIndex -= additionalBytes;
		final int bitOffset = computeBitOffset(bitIndex, Float.SIZE, byteIndex, numBytes, additionalBytes);
		int bits;
		if(numBytes == Float.BYTES) {
			bits = (int) read(store.getInt(byteIndex), bitOffset, Float.SIZE);
		} else {
			bits = (int) read(store.getLong(byteIndex), bitOffset, Float.SIZE);
		}
		return Float.intBitsToFloat(bits);
	}

	@Override
	public void setReal32Bits(int bitIndex, float value) {
		int byteIndex = byteIndex(bitIndex);
		final int numBytes = getMinimumBytesFor(bitIndex, Float.SIZE);
		final int additionalBytes = getAdditionalBytes(byteSize(), byteIndex, numBytes);
		byteIndex -= additionalBytes;
		final int bitOffset = computeBitOffset(bitIndex, Float.SIZE, byteIndex, numBytes, additionalBytes);
		final int bits = Float.floatToIntBits(value);
		if(numBytes == Float.BYTES) {
			final int newValue = (int) write(getAlignedInteger(bitIndex), bits, bitOffset, Float.SIZE);
			store.setInt(byteIndex, newValue);
		} else {
			final long newValue = write(getAlignedLong(bitIndex), bits, bitOffset, Float.SIZE);
			store.setLong(byteIndex, newValue);
		}
	}

	@Override
	public float getAlignedReal32Bits(int bitIndex) {
		return store.getFloat(byteIndex(bitIndex));
	}

	@Override
	public void setAlignedReal32Bits(int bitIndex, float value) {
		store.setFloat(byteIndex(bitIndex), value);
	}

	// ================================= DOUBLE =================================

	@Override
	public double getReal64Bits(int bitIndex) {
		int byteIndex = byteIndex(bitIndex);
		final int numBytes = getMinimumBytesFor(bitIndex, Double.SIZE);
		final int additionalBytes = getAdditionalBytes(byteSize(), byteIndex, numBytes);
		byteIndex -= additionalBytes;
		final int bitOffset = computeBitOffset(bitIndex, Double.SIZE, byteIndex, numBytes, additionalBytes);
		final long bits = read(store.getLong(byteIndex), bitOffset, Double.SIZE);
		return Double.longBitsToDouble(bits);
	}

	@Override
	public void setReal64Bits(int bitIndex, double value) {
		int byteIndex = byteIndex(bitIndex);
		final int numBytes = getMinimumBytesFor(bitIndex, Double.SIZE);
		final int additionalBytes = getAdditionalBytes(byteSize(), byteIndex, numBytes);
		byteIndex -= additionalBytes;
		final int bitOffset = computeBitOffset(bitIndex, Double.SIZE, byteIndex, numBytes, additionalBytes);
		final long bits = Double.doubleToLongBits(value);
		final long newValue = write(getAlignedLong(bitIndex), bits, bitOffset, Double.SIZE);
		store.setLong(byteIndex, newValue);
	}

	@Override
	public double getAlignedReal64Bits(int bitIndex) {
		return store.getDouble(byteIndex(bitIndex));
	}

	@Override
	public void setAlignedReal64Bits(int bitIndex, double value) {
		store.setDouble(byteIndex(bitIndex), value);
	}

	// ================================= ================================= =================================

	private long getNBits(int bitIndex, int bitCount) {
		long value;
		int byteIndex = byteIndex(bitIndex);
		final int numBytes = getMinimumBytesFor(bitIndex, bitCount);
		final int additionalBytes = getAdditionalBytes(byteSize(), byteIndex, numBytes);
		byteIndex -= additionalBytes;
		final int bitOffset = computeBitOffset(bitIndex, bitCount, byteIndex, numBytes, additionalBytes);
		switch(numBytes) {
			case Byte.BYTES:
				value = store.getByte(byteIndex);
				value &= 0xFF;
				break;
			case Short.BYTES:
				value = store.getShort(byteIndex);
				value &= 0xFFFF;
				break;
			case Integer.BYTES:
				value = store.getInt(byteIndex);
				value &= 0xFFFFFFFF;
				break;
			case Long.BYTES:
				value = store.getLong(byteIndex);
				break;
			default:
				throw misAlignmentError(numBytes);
		}
		return read(value, bitOffset, bitCount);
	}

	private void setNBits(long value, int bitIndex, int bitCount) {
		int byteIndex = byteIndex(bitIndex);
		final int numBytes = getMinimumBytesFor(bitIndex, bitCount);
		final int additionalBytes = getAdditionalBytes(byteSize(), byteIndex, numBytes);
		byteIndex -= additionalBytes;
		final int bitOffset = computeBitOffset(bitIndex, bitCount, byteIndex, numBytes, additionalBytes);
		switch(numBytes) {
			case Byte.BYTES:
				setInt8(bitIndex, byteIndex, bitOffset, bitCount, value);
				break;
			case Short.BYTES:
				setInt16(bitIndex, byteIndex, bitOffset, bitCount, value);
				break;
			case Integer.BYTES:
				setInt32(bitIndex, byteIndex, bitOffset, bitCount, value);
				break;
			case Long.BYTES:
				setInt64(bitIndex, byteIndex, bitOffset, bitCount, value);
				break;
			default:
				throw misAlignmentError(numBytes);
		}
	}

	private UnsupportedOperationException misAlignmentError(int numBytes) {
		return new UnsupportedOperationException("Value will use " + numBytes + " bytes due to misalignment." +
				" Align the value to a bit position multiple of " + Long.SIZE);
	}

	protected abstract int computeBitOffset(int bitIndex, int bitCount, int byteIndex, int numBytes, int additionalBytes);

	private void setInt8(int bitIndex, int byteIndex, int bitOffset, int bitCount, long value) {
		final byte oldValue = getAlignedByte(bitIndex);
		final byte newValue = (byte)(write(oldValue, (byte)value, bitOffset, bitCount));
		store.setByte(byteIndex, newValue);
	}

	private void setInt16(int bitIndex,int byteIndex, int bitOffset, int bitCount, long value) {
		final short oldValue = getAlignedShort(bitIndex);
		final short newValue = (short)(write(oldValue, (short)value, bitOffset, bitCount));
		store.setShort(byteIndex, newValue);
	}

	private void setInt32(int bitIndex,int byteIndex, int bitOffset, int bitCount, long value) {
		final int oldValue = getAlignedInteger(bitIndex);
		final int newValue = (int)(write(oldValue, (int)value, bitOffset, bitCount));
		store.setInt(byteIndex, newValue);
	}

	private void setInt64(int bitIndex, int byteIndex, int bitOffset, int bitCount, long value) {
		final long oldValue = getAlignedLong(bitIndex);
		final long newValue = write(oldValue, value, bitOffset, bitCount);
		store.setLong(byteIndex, newValue);
	}

	@Override
	public final void clear() {
		store.clear();
	}

	@Override
	public String toBinaryString() {
		return toBinaryString(0);
	}

	@Override
	public String toBinaryString(int splitSize) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < byteSize(); i++) {
			sb.append(BitUtils.toBinaryString(store.getByte(i) & 0xFF, Byte.SIZE, splitSize));
		}
		return sb.toString();
	}

	@Override
	public String toHexString() {
		StringBuilder sb = new StringBuilder();
		for (int i = (int) (endOffset() - 1); i >= 0; i--) {
			sb.insert(0, String.format("%02X", store.getByte(i) & 0xFF));
		}
		return "0x" + sb.toString();
	}

	@Override
	public String toString() {
		return toBinaryString(Byte.SIZE);//toHexString();
	}

	@Override
	public boolean isReadOnly() {
		return store instanceof ReadOnlyByteStore;
	}
}

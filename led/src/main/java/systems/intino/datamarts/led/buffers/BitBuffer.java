package systems.intino.datamarts.led.buffers;


import systems.intino.datamarts.led.util.OffHeapObject;

public interface BitBuffer extends OffHeapObject {

	long address();
	void invalidate();

	@Override
	long byteSize();
	long bitCount();
	long baseOffset();
	long endOffset();

	boolean getBoolean(int bitIndex);
	void setBoolean(int bitIndex, boolean value);

	byte getByteNBits(int bitIndex, int bitCount);
	void setByteNBits(int bitIndex, int bitCount, byte value);
	short getUByteNBits(int bitIndex, int bitCount);
	void setUByteNBits(int bitIndex, int bitCount, short value);
	byte getAlignedByte(int bitIndex);
	void setAlignedByte(int bitIndex, byte value);
	short getAlignedUByte(int bitIndex);
	void setAlignedUByte(int bitIndex, short value);

	short getShortNBits(int bitIndex, int bitCount);
	void setShortNBits(int bitIndex, int bitCount, short value);
	int getUShortNBits(int bitIndex, int bitCount);
	void setUShortNBits(int bitIndex, int bitCount, int value);
	short getAlignedShort(int bitIndex);
	void setAlignedShort(int bitIndex, short value);
	int getAlignedUShort(int bitIndex);
	void setAlignedUShort(int bitIndex, int value);

	int getIntegerNBits(int bitIndex, int bitCount);
	void setIntegerNBits(int bitIndex, int bitCount, int value);
	long getUIntegerNBits(int bitIndex, int bitCount);
	void setUIntegerNBits(int bitIndex, int bitCount, long value);
	int getAlignedInteger(int bitIndex);
	void setAlignedInteger(int bitIndex, int value);
	long getAlignedUInteger(int bitIndex);
	void setAlignedUInteger(int bitIndex, long value);

	long getLongNBits(int bitIndex, int bitCount);
	void setLongNBits(int bitIndex, int bitCount, long value);
	long getULongNBits(int bitIndex, int bitCount);
	void setULongNBits(int bitIndex, int bitCount, long value);
	long getAlignedLong(int bitIndex);
	void setAlignedLong(int bitIndex, long value);
	long getAlignedULong(int bitIndex);
	void setAlignedULong(int bitIndex, long value);

	float getReal32Bits(int bitIndex);
	void setReal32Bits(int bitIndex, float value);
	float getAlignedReal32Bits(int bitIndex);
	void setAlignedReal32Bits(int bitIndex, float value);

	double getReal64Bits(int bitIndex);
	void setReal64Bits(int bitIndex, double value);
	double getAlignedReal64Bits(int bitIndex);
	void setAlignedReal64Bits(int bitIndex, double value);

	void clear();
	String toBinaryString();
	String toBinaryString(int splitSize);
	String toHexString();
	@Override
	String toString();
	boolean isReadOnly();
}

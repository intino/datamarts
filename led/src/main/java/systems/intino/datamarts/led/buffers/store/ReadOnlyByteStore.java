package systems.intino.datamarts.led.buffers.store;

import java.nio.ByteOrder;

public final class ReadOnlyByteStore implements ByteStore {

	private static final String UNSUPPORTED_OPERATION_EXCEPTION_MESSAGE =
			"This ByteStore is read-only. Probably the original ByteStore was invalidated";


	private final ByteStore store;

	public ReadOnlyByteStore(ByteStore store) {
		this.store = store;
	}

	@Override
	public ByteOrder order() {
		return store.order();
	}

	@Override
	public Object storeImpl() {
		return store.storeImpl();
	}

	@Override
	public long baseOffset() {
		return store.baseOffset();
	}

	@Override
	public byte getByte(int byteIndex) {
		return store.getByte(byteIndex);
	}

	@Override
	public void setByte(int byteIndex, byte value) {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_EXCEPTION_MESSAGE);
	}

	@Override
	public short getShort(int byteIndex) {
		return store.getShort(byteIndex);
	}

	@Override
	public void setShort(int byteIndex, short value) {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_EXCEPTION_MESSAGE);
	}

	@Override
	public char getChar(int byteIndex) {
		return store.getChar(byteIndex);
	}

	@Override
	public void setChar(int byteIndex, char value) {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_EXCEPTION_MESSAGE);
	}

	@Override
	public int getInt(int byteIndex) {
		return store.getInt(byteIndex);
	}

	@Override
	public void setInt(int byteIndex, int value) {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_EXCEPTION_MESSAGE);
	}

	@Override
	public long getLong(int byteIndex) {
		return store.getLong(byteIndex);
	}

	@Override
	public void setLong(int byteIndex, long value) {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_EXCEPTION_MESSAGE);
	}

	@Override
	public float getFloat(int byteIndex) {
		return store.getFloat(byteIndex);
	}

	@Override
	public void setFloat(int byteIndex, float value) {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_EXCEPTION_MESSAGE);
	}

	@Override
	public double getDouble(int byteIndex) {
		return store.getDouble(byteIndex);
	}

	@Override
	public void setDouble(int byteIndex, double value) {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_EXCEPTION_MESSAGE);
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_EXCEPTION_MESSAGE);
	}

	@Override
	public ByteStore slice(long baseOffset, long size) {
		throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_EXCEPTION_MESSAGE);
	}

	@Override
	public long address() {
		return store.address();
	}

	@Override
	public long byteSize() {
		return store.byteSize();
	}
}

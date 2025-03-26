package systems.intino.datamarts.led.buffers;

import systems.intino.datamarts.led.buffers.store.ByteStore;

import static systems.intino.datamarts.led.util.BitUtils.offsetOf;

public class BigEndianBitBuffer extends AbstractBitBuffer {

	public BigEndianBitBuffer(ByteStore store) {
		super(store);
	}

	@Override
	protected int computeBitOffset(int bitIndex, int bitCount, int byteIndex, int numBytes, int additionalBytes) {
		return (numBytes * Byte.SIZE - bitCount - offsetOf(bitIndex)) - additionalBytes * Byte.SIZE;
	}

}
package systems.intino.dataformats.led.buffers;

import systems.intino.dataformats.led.buffers.store.ByteStore;

import static systems.intino.dataformats.led.util.BitUtils.offsetOf;

public class LittleEndianBitBuffer extends AbstractBitBuffer {

	public LittleEndianBitBuffer(ByteStore store) {
		super(store);
	}

	@Override
	protected int computeBitOffset(int bitIndex, int bitCount, int byteIndex, int numBytes, int additionalBytes) {
		return offsetOf(bitIndex) + additionalBytes * Byte.SIZE;
	}
}
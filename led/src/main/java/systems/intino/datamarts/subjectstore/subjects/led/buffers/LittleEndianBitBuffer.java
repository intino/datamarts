package systems.intino.datamarts.subjectstore.subjects.led.buffers;

import systems.intino.datamarts.subjectstore.subjects.led.buffers.store.ByteStore;

import static systems.intino.datamarts.subjectstore.subjects.led.util.BitUtils.offsetOf;

public class LittleEndianBitBuffer extends AbstractBitBuffer {

	public LittleEndianBitBuffer(ByteStore store) {
		super(store);
	}

	@Override
	protected int computeBitOffset(int bitIndex, int bitCount, int byteIndex, int numBytes, int additionalBytes) {
		return offsetOf(bitIndex) + additionalBytes * Byte.SIZE;
	}
}
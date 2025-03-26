package systems.intino.datamarts.led.util;


import org.apache.commons.lang3.StringUtils;

import static java.lang.Math.abs;
import static java.lang.Math.min;

public final class BitUtils {

	/**
	 * Reads a region of a value, bitwise. It will return bitCount bits of n from offset position.
	 *
	 * */
	public static long read(long n, int offset, int bitCount) {
		return (n >> offset) & ones(bitCount);
	}

	/**
	 * Writes a region of a value, bitwise. It will write bitCount bits of value into n, at position offset.
	 *
	 * */
	public static long write(long n, long value, int offset, int bitCount) {
		return (n & zeros(bitCount, offset)) // Clear writable part of the old value
				| (value << offset); // Writes only the necessary bits at their corresponding positions
	}

	/**
	 * Returns a bitmask consisting of zeros except the bit at position pos
	 * */
	public static long bitmask(int pos) {
		return pos == Long.SIZE ? -1L : (1L << pos);
	}

	/**
	 * Returns a bitmask consisting of ones from the position 0 to pos (from right to left).
	 * */
	public static long ones(int pos) {
		final long bitmask = bitmask(pos);
		return bitmask == -1 ? bitmask : bitmask - 1;
	}

	/**
	 * Returns a bitmask consisting of count ones from the position fromPos
	 *
	 * */
	public static long ones(int count, int fromPos) {
		return (ones(count) << fromPos);
	}

	/**
	 * Returns a bitmask consisting of count zeros from the position fromPos
	 *
	 * */
	public static long zeros(int count, int fromPos) {
		return ~ones(count, fromPos);
	}

	public static String toBinaryString(long value, int padding) {
		return StringUtils.leftPad(Long.toBinaryString(value), padding, '0');
	}

	public static int byteIndex(int bitIndex) {
		return bitIndex / Byte.SIZE;
	}

	public static int bitIndex(int byteIndex) {
		return byteIndex * Byte.SIZE;
	}

	public static int offsetOf(int bitIndex) {
		return bitIndex % Byte.SIZE;
	}

	public static int roundUp2(int n, int multiple) {
		return (n + multiple - 1) & (-multiple);
	}

	public static int bitsUsed(long value) {
		return Long.SIZE - Long.numberOfLeadingZeros(value);
	}

	public static long maxPossibleNumber(int numberOfBits) {
		final long x = (long) Math.pow(2, numberOfBits);
		return x / 2 - 1;
	}

	public static int getAdditionalBytes(long bufferSize, int byteIndex, int numBytes) {
		return (int) abs(min(bufferSize - byteIndex - numBytes, 0));
	}

	public static int getMinimumBytesFor(int bitIndex, int bitCount) {
		final int bitOffset = offsetOf(bitIndex);
		final int numBytes = (int)Math.ceil((bitOffset + bitCount) / 8.0);
		return roundSize(numBytes);
	}

	public static int roundSize(int n) {
		if (n == 1 || n == 2 || n == 4 || n == 8) return n;
		if (n < 4) return 4;
		return Math.max(n, 8);
	}

	// 1 < nBits <= 64
	public static long extendSign(long n, int nBits) {
		final long shift = 64 - nBits;
		return (n << shift) >> shift;
	}

	public static String toBinaryString(long value, int padding, int splitSize) {

		final String str = toBinaryString(value, padding);

		if (splitSize == 0) {
			return str;
		}

		StringBuilder sb = new StringBuilder(str);

		int offset = splitSize;
		for (int i = 0; i < str.length() / splitSize; i++) {
			sb.insert(offset, ' ');
			offset += splitSize + 1;
		}

		return sb.toString();
	}

	private BitUtils() {
	}
}

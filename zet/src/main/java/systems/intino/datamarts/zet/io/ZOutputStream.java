package systems.intino.datamarts.zet.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class ZOutputStream extends OutputStream {
	private DataOutputStream output;
	private long base = -1;
	private byte[] data = new byte[256];
	private int count = 0;
	private int size = 0;

	public ZOutputStream(OutputStream outputStream) {
		this.output = new DataOutputStream(outputStream);
	}

	public void write(int b) {
	}

	public void writeLong(long id) {
		this.base(id >> 8);
		if (isRepeated((byte) id)) return;
		this.data[this.count++] = (byte) (id);
		this.size++;
	}

	private boolean isRepeated(byte b) {
		return count > 0 && this.data[this.count - 1] == b;
	}

	private void base(long base) {
		try {
			if (this.base == base) return;
			writeData();
			writeBase(base);
			this.base = base;
		} catch (IOException e) {
		}
	}

	private void writeBase(long base) throws IOException {
		int level = this.base >= 0 ? level(base, this.base) : level(base);
		output.writeByte(level);
		for (int i = level - 1; i >= 0; i--) {
			byte b = (byte) (base >> (i << 3));
			output.writeByte(b);
		}
	}

	private int level(long base) {
		return base != 0 ? level(base >> 8) + 1 : 0;
	}

	private int level(long a, long b) {
		return a != b ? level(a >> 8, b >> 8) + 1 : 0;
	}

	private void writeData() throws IOException {
		if (base < 0) return;
		output.writeByte(count);
		for (int i = 0; i < count; i++) output.writeByte(data[i]);
		count = 0;
	}

	@Override
	public void close() throws IOException {
		writeData();
		output.writeLong(0xFFFFFFFFFFFFFFFFL);
		output.writeLong(size);
		output.close();
	}
}

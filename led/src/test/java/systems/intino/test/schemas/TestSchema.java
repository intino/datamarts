package systems.intino.test.schemas;

import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.SchemaFactory;
import systems.intino.datamarts.led.buffers.store.ByteBufferStore;
import systems.intino.datamarts.led.buffers.store.ByteStore;
import systems.intino.datamarts.led.util.memory.MemoryAddress;
import systems.intino.datamarts.led.util.memory.MemoryUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static systems.intino.datamarts.led.util.BitUtils.roundUp2;

public class TestSchema extends Schema {

	public enum SimpleWord {
		A(1), B(2), C(1);
		int value;

		SimpleWord(int value) {
			this.value = value;
		}

		public int value() {
			return value;
		}
	}

	public static final int ID_OFFSET = 0;
	public static final int ID_BITS = Long.SIZE;
	public static final int A_OFFSET = ID_OFFSET + ID_BITS;
	public static final int A_BITS = 10;
	public static final int B_OFFSET = A_OFFSET + A_BITS;
	public static final int B_BITS = Integer.SIZE;
	public static final int C_OFFSET = B_OFFSET + B_BITS;
	public static final int C_BITS = Float.SIZE;
	public static final int D_OFFSET = C_OFFSET + C_BITS;
	public static final int D_BITS = 29;
	public static final int E_OFFSET = roundUp2(D_OFFSET + D_BITS, Long.SIZE);
	public static final int E_BITS = 64;
	public static final int F_OFFSET = roundUp2(E_OFFSET + E_BITS, Double.SIZE);
	public static final int F_BITS = Double.SIZE;
	public static final int G_OFFSET = F_OFFSET + F_BITS;
	public static final int G_BITS = 20;
	public static final int H_OFFSET = G_OFFSET + G_BITS;
	public static final int H_BITS = 1;
	public static final int I_OFFSET = H_OFFSET + H_BITS;
	public static final int I_BITS = 9;
	public static final int J_OFFSET = I_OFFSET + I_BITS;
	public static final int J_BITS = 9;
	// private long id;
	// private short a;
	// private int b;
	// private float c;
	// private int d;
	// private long e;
	// private double f;
	// private byte g;
	public static final int SIZE = (int) Math.ceil((J_OFFSET + J_BITS) / (float) Byte.SIZE);

	public static final UUID SERIAL_UUID = UUID.nameUUIDFromBytes(TestSchema.class.getName().getBytes());

	public static final SchemaFactory<TestSchema> FACTORY = new SchemaFactory<>(TestSchema.class) {
		@Override
		public TestSchema newInstance(ByteStore store) {
			return new TestSchema(store);
		}
	};

	public TestSchema(ByteStore store) {
		super(store);
	}

	public TestSchema(ByteOrder byteOrder) {
		super(getDefaultByteStore(byteOrder));
	}

	private static ByteStore getDefaultByteStore(ByteOrder byteOrder) {
		ByteBuffer buffer = MemoryUtils.allocBuffer(SIZE, byteOrder);
		MemoryAddress address = MemoryAddress.of(buffer);
		return new ByteBufferStore(buffer, address, 0, buffer.capacity());
	}

	int[] fieldSizes() {
		return new int[] {
				ID_BITS,
				A_BITS,
				B_BITS,
				C_BITS,
				D_BITS,
				E_BITS,
				F_BITS,
				G_BITS,
				H_BITS,
				ID_BITS,
				J_BITS
		};
	}

	@Override
	public long id() {
		return bitBuffer.getAlignedLong(0);
	}

	@Override
	public int size() {
		return SIZE;
	}

	@Override
	public UUID serialUUID() {
		return SERIAL_UUID;
	}

	public TestSchema id(Number id) {
		bitBuffer.setAlignedLong(ID_OFFSET, id.longValue());
		return this;
	}

	public short a() {
		return bitBuffer.getShortNBits(A_OFFSET, A_BITS);
	}

	public TestSchema a(Number a) {
		bitBuffer.setShortNBits(A_OFFSET, A_BITS, a.shortValue());
		return this;
	}

	public int b() {
		return bitBuffer.getIntegerNBits(B_OFFSET, B_BITS);
	}

	public TestSchema b(Number b) {
		bitBuffer.setIntegerNBits(B_OFFSET, B_BITS, b.intValue());
		return this;
	}

	public float c() {
		return bitBuffer.getReal32Bits(C_OFFSET);
	}

	public TestSchema c(Number c) {
		bitBuffer.setReal32Bits(C_OFFSET, c.floatValue());
		return this;
	}

	public int d() {
		return bitBuffer.getIntegerNBits(D_OFFSET, D_BITS);
	}

	public TestSchema d(Number d) {
		bitBuffer.setIntegerNBits(D_OFFSET, D_BITS, d.intValue());
		return this;
	}

	public long e() {
		return bitBuffer.getLongNBits(E_OFFSET, E_BITS);
	}

	public TestSchema e(Number e) {
		bitBuffer.setLongNBits(E_OFFSET, E_BITS, e.longValue());
		return this;
	}

	public double f() {
		return bitBuffer.getAlignedReal64Bits(F_OFFSET);
	}

	public TestSchema f(Number f) {
		bitBuffer.setAlignedReal64Bits(F_OFFSET, f.doubleValue());
		return this;
	}

	public byte g() {
		return bitBuffer.getByteNBits(G_OFFSET, G_BITS);
	}

	public TestSchema g(Number g) {
		bitBuffer.setIntegerNBits(G_OFFSET, G_BITS, g.intValue());
		return this;
	}

	public boolean h() {
		return bitBuffer.getBoolean(H_OFFSET);
	}

	public TestSchema h(boolean h) {
		bitBuffer.setBoolean(H_OFFSET, h);
		return this;
	}

	public String i() {
		final int word = bitBuffer.getByteNBits(I_OFFSET, I_BITS);
		return word == NULL ? null : ResourceWord.values().get(word);
	}

	public TestSchema i(String i) {
		bitBuffer.setByteNBits(I_OFFSET, I_BITS, (byte)(i == null ? NULL : ResourceWord.indexOf(i)));
		return this;
	}

	public Boolean j() {
		final byte word = bitBuffer.getByteNBits(I_OFFSET, I_BITS);
		return word == NULL ? null : word == 1;
	}

	public TestSchema j(Boolean i) {
		bitBuffer.setByteNBits(J_OFFSET, J_BITS, (byte)(i == null ? NULL : i ? 1 : 2));
		return this;
	}

	@Override
	public String toString() {
		return "TestSchemaObj{"
				+ "id = " + id()
				+ ", a = " + a()
				+ ", b = " + b()
				+ ", c = " + c()
				+ ", d = " + d()
				+ ", e = " + e()
				+ ", f = " + f()
				+ ", g = " + g()
				+ "}";
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(id());
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof TestSchema)) return false;
		TestSchema o = (TestSchema) obj;
		return id() == o.id()
				&& a() == o.a()
				&& b() == o.b()
				&& c() == o.c()
				&& d() == o.d()
				&& e() == o.e()
				&& f() == o.f()
				&& g() == o.g();
	}

	public static class ResourceWord {
		private static final Map<Integer, String> values;

		static {
			values = new BufferedReader(new InputStreamReader(ResourceWord.class.getResourceAsStream("ResourceWord.tsv"))).lines().map(l -> l.split("\t")).collect(Collectors.toMap(l -> Integer.parseInt(l[0]), l -> l[1]));
		}

		public static Map<Integer, String> values() {
			return values;
		}

		public static long indexOf(String i) {
			Map.Entry<Integer, String> e = values.entrySet().stream().filter(en -> en.getValue().equals(i)).findFirst().orElse(null);
			return e == null ? NULL : e.getKey();
		}
	}
}

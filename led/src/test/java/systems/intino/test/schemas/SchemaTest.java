package systems.intino.test.schemas;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteOrder;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static systems.intino.datamarts.led.util.BitUtils.maxPossibleNumber;
import static systems.intino.datamarts.led.util.BitUtils.toBinaryString;
import static systems.intino.datamarts.led.util.memory.MemoryUtils.memset;
import static systems.intino.test.schemas.TestSchema.*;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class SchemaTest {

	@Parameterized.Parameters
	public static Collection<?> getParameters() {
		return List.of(BIG_ENDIAN, LITTLE_ENDIAN);
	}

	private final Random random;
	private final ByteOrder byteOrder;
	private TestSchema schema;

	public SchemaTest(ByteOrder byteOrder) {
		this.byteOrder = byteOrder;
		random = new Random(System.nanoTime());
	}

	@Before
	public void setup() {
		schema = new TestSchema(byteOrder);
		// Random values in memory
		for (int i = 0; i < SIZE; i++) memset(schema.address(), 1, random.nextInt() - random.nextInt());
	}

	@Test
	//@Ignore
	//@Test(expected = UnsupportedOperationException.class)
	public void testSetFieldDoesNotModifyOtherFields() {
		schema = new TestSchema(byteOrder);
		List<Function<TestSchema, Number>> getters = getters();
		List<BiConsumer<TestSchema, Number>> setters = setters();
		List<Function<Number, Number>> interpreter = interpreters();
		Number[] lastValues = new Number[getters.size()];
		Random random = new Random(System.nanoTime());

		for(int i = 0;i < setters.size();i++) {
			char c = (char)((char)(i-1) + 'a');
			if(i == 0) {
				System.out.println("=== id ===");
			} else {
				System.out.println("=== " + c + " ===");
			}
			BiConsumer<TestSchema, Number> setter = setters.get(i);
			final Function<TestSchema, Number> getter = getters.get(i);
			final Function<Number, Number> interpret = interpreter.get(i);
			Number value = interpret.apply(maxPossibleNumber(schema.fieldSizes()[i]));
			setter.accept(schema, value);
			// Assert the value is correctly set
			assertEquals(value, getter.apply(schema));
			lastValues[i] = value;
			//System.out.println(schema);
			// Check if other fields have been affected
			for(int j = 0;j < getters.size();j++) {
				if(i == j) continue;
				if(lastValues[j] == null) continue;
				String c2 = j == 0 ? "id" : String.valueOf((char)((char)(j-1) + 'a'));
				System.out.println("\tChecking " + c2);
				Number expected = interpreter.get(j).apply(lastValues[j]);
				Number actual = getters.get(j).apply(schema);
				assertEquals("\n"+toBinaryString(expected.longValue(), 64, 8)
								+ "\n"+toBinaryString(actual.longValue(), 64, 8),
						expected, actual);
			}
		}
	}

	private List<Function<Number, Number>> interpreters() {
		return List.of(
				Number::longValue,
				Number::shortValue,
				Number::intValue,
				Number::floatValue,
				Number::intValue,
				Number::longValue,
				Number::doubleValue,
				Number::byteValue
		);
	}

	private List<Function<TestSchema, Number>> getters() {
		return List.of(
				TestSchema::id,
				TestSchema::a,
				TestSchema::b,
				TestSchema::c,
				TestSchema::d,
				TestSchema::e,
				TestSchema::f,
				TestSchema::g
		);
	}

	private List<BiConsumer<TestSchema, Number>> setters() {
		return List.of(
				TestSchema::id,
				TestSchema::a,
				TestSchema::b,
				TestSchema::c,
				TestSchema::d,
				TestSchema::e,
				TestSchema::f,
				TestSchema::g
		);
	}

	@Test
	public void id() {
		final long id = random.nextLong();
		schema.id(id);
		assertEquals(id, schema.id());
	}

	@Test
	public void a() {
		short a = (short) maxPossibleNumber(A_BITS);
		schema.a(a);
		assertEquals(a, schema.a());
	}

	@Test
	public void b() {
		int b = (int) maxPossibleNumber(B_BITS);
		schema.b(b);
		assertEquals(b, schema.b());
	}

	@Test
	public void c() {
		for(int i = 0;i < 10;i++) {
			float c = (float) Math.random() * (i + 1);
			schema.c(c);
			assertEquals(c, schema.c(), 0.0f);
		}
	}

	@Test
	public void d() {
		int d = (int) maxPossibleNumber(D_BITS);
		schema.d(d);
		assertEquals(d, schema.d());
	}

	@Test//(expected = UnsupportedOperationException.class)
	public void e() {
		long e = Math.abs(new Random().nextInt());//maxPossibleNumber(E_BITS) / 2;
		schema.e(e);
		assertEquals(schema.toString(), e, schema.e());
	}

	@Test
	public void f() {
		double f = maxPossibleNumber(F_BITS);
		schema.f(f);
		assertEquals(f, schema.f(), 0.0);
	}

	@Test
	public void g() {
		byte g = (byte) maxPossibleNumber(G_BITS);
		schema.g(g);
		assertEquals(g, schema.g());
	}

	@Test
	public void h() {
		boolean value = true;
		schema.h(value);
		assertEquals(value, schema.h());
	}

	private int factorial(int n) {
		int result = 1;
		int i = n;
		while (i > 1) {
			result *= i;
			--i;
		}
		return result;
	}
}
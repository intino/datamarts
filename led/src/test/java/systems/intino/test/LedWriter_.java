package systems.intino.test;

import systems.intino.datamarts.led.*;
import systems.intino.datamarts.led.allocators.SchemaAllocator;
import systems.intino.datamarts.led.allocators.indexed.ListAllocator;
import systems.intino.datamarts.led.leds.IteratorLedStream;
import systems.intino.test.schemas.TestSchema;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static systems.intino.datamarts.led.util.BitUtils.maxPossibleNumber;
import static systems.intino.test.schemas.TestSchema.*;
import static org.junit.Assert.assertEquals;

public class LedWriter_ {

	private static final int NUM_ELEMENTS = 100_000;
	private static final File tempFile = new File("temp/snappy_test.led");
	private static final Random RANDOM = new Random();
	private static final AtomicLong RPU = new AtomicLong();

	@BeforeClass
	public static void beforeClass() throws Exception {
		tempFile.getParentFile().mkdirs();
	}

	@AfterClass
	public static void afterClass() throws Exception {
		tempFile.delete();
	}

	@Test
	public void should_write_and_read() {
		List<TestSchema> original = generateTestSchemaObjs(
				new ListAllocator<>(NUM_ELEMENTS / 10, TestSchema.SIZE, TestSchema.class))
				.collect(Collectors.toList());
		original.sort(Comparator.comparingLong(TestSchema::id));
		write(original);
		read(original);
	}

	private void write(List<TestSchema> original) {
		long start;
		LedStream<TestSchema> led = new IteratorLedStream<>(TestSchema.class, original.iterator());
		start = System.currentTimeMillis();
		new LedWriter(tempFile).write(Led.fromLedStream(led));
		System.out.println(">> Serialized " + NUM_ELEMENTS + "(" + TestSchema.SIZE + " bytes each) in "
				+ (System.currentTimeMillis() - start) / 1000 + " seconds");
	}

	private void read(List<TestSchema> original) {
		LedReader reader = new LedReader(tempFile);
		assertEquals(original.size(), reader.size());
		long start;
		start = System.currentTimeMillis();
		Led<TestSchema> led = reader.readAll(TestSchema.class);
		System.out.println(">> Deserialized " + NUM_ELEMENTS + "(" + TestSchema.SIZE + " bytes each) in "
				+ (System.currentTimeMillis() - start) / 1000 + " seconds");
		AtomicInteger size = new AtomicInteger();
		IntStream.range(0, original.size()).forEach(i -> {
			assertEquals(original.get(i), led.schema(i));
			size.getAndIncrement();
		});
		assertEquals(original.size(), size.get());
	}

	private Stream<TestSchema> generateTestSchemaObjs(SchemaAllocator<TestSchema> allocator) {
		Stream<TestSchema> stream = IntStream.range(0, NUM_ELEMENTS)
				.parallel()
				.unordered()
				.mapToObj(i -> allocator.malloc()
						.id(RPU.incrementAndGet())
						.a((short) RANDOM.nextInt((int) maxPossibleNumber(A_BITS)))
						.b(RANDOM.nextInt((int) maxPossibleNumber(B_BITS)))
						.c(RANDOM.nextFloat() * RANDOM.nextInt())
						.d(RANDOM.nextInt((int) maxPossibleNumber(D_BITS)))
						.e(RANDOM.nextInt(Math.abs((int) maxPossibleNumber(E_BITS))))
						.f(RANDOM.nextDouble() * RANDOM.nextInt())
						.g((byte) RANDOM.nextInt((int) maxPossibleNumber(G_BITS)))
				);
		return stream.onClose(allocator::clear);
	}

	@After
	public void tearDown() {
		tempFile.delete();
	}
}
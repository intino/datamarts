package systems.intino.test.allocators;

import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.DefaultAllocator;
import systems.intino.datamarts.led.allocators.SchemaAllocator;
import systems.intino.datamarts.led.allocators.indexed.ArrayAllocator;
import systems.intino.datamarts.led.allocators.indexed.ListAllocator;
import systems.intino.datamarts.led.allocators.stack.StackAllocators;
import systems.intino.datamarts.led.allocators.stack.StackListAllocator;
import systems.intino.test.schemas.TestSchema;
import org.junit.Ignore;

@Ignore
public class TestAllocatorsSpeed {

	private static final int NUM_ELEMENTS = 40_000_000;
	private static final float MS_TO_SECONDS = 1000.0f;

	public static void main(String[] args) throws InterruptedException {

		warmUp();

		final long defaultAllocatorTime = benchmark(new DefaultAllocator<>(TestSchema.SIZE, TestSchema.class));

		final long ustackAllocatorTime = benchmark(StackAllocators.unmanagedStackAllocator(TestSchema.SIZE, NUM_ELEMENTS, TestSchema.class));

		final long stackAllocatorTime = benchmark(StackAllocators.managedStackAllocator(TestSchema.SIZE, NUM_ELEMENTS, TestSchema.class));

		final long stackListAllocatorTime = benchmark(new StackListAllocator<>(NUM_ELEMENTS / 10, TestSchema.SIZE,
				TestSchema.class, StackAllocators::managedStackAllocator));

		final long stackListAllocatorReservedTime = benchmark(new StackListAllocator<>(10, NUM_ELEMENTS / 10, TestSchema.SIZE,
				TestSchema.class, StackAllocators::managedStackAllocator));

		final long arrayAllocatorTime = benchmark(new ArrayAllocator<>(10, NUM_ELEMENTS / 10, TestSchema.SIZE, TestSchema.class));

		final long listAllocatorTime = benchmark(new ListAllocator<>(NUM_ELEMENTS / 10, TestSchema.SIZE, TestSchema.class));


		float max = Math.max(Math.max(defaultAllocatorTime, stackAllocatorTime), stackListAllocatorTime);
		max = Math.max(max, listAllocatorTime);
		max = Math.max(max, arrayAllocatorTime);
		max = Math.max(max, ustackAllocatorTime);


		final float defaultAllocatorRelativeTime = 100 - (defaultAllocatorTime / max * 100.0f);
		final float stackAllocatorRelativeTime = 100 - (stackAllocatorTime / max * 100.0f);
		final float ustackAllocatorRelativeTime = 100 - (ustackAllocatorTime / max * 100.0f);
		final float stackListAllocatorRelativeTime = 100 - (stackListAllocatorTime / max * 100.0f);
		final float stackListAllocatorReservedRelativeTime = 100 - (stackListAllocatorReservedTime / max * 100.0f);
		final float listAllocatorRelativeTime = 100 - (listAllocatorTime / max * 100.0f);
		final float arrayAllocatorRelativeTime = 100 - (arrayAllocatorTime / max * 100.0f);


		System.out.println("Test Allocator Speed (" + NUM_ELEMENTS + " elements, element size in bytes = " + TestSchema.SIZE + ")");
		System.out.println(">> DefaultAllocator: " + defaultAllocatorTime / MS_TO_SECONDS + " seconds (" + defaultAllocatorRelativeTime + "% faster)");
		System.out.println(">> Managed StackAllocator: " + stackAllocatorTime / MS_TO_SECONDS + " seconds (" + stackAllocatorRelativeTime + "% faster)");
		System.out.println(">> Unmanaged StackAllocator: " + ustackAllocatorTime / MS_TO_SECONDS + " seconds (" + ustackAllocatorRelativeTime + "% faster)");
		System.out.println(">> StackListAllocator: " + stackListAllocatorTime / MS_TO_SECONDS + " seconds (" + stackListAllocatorRelativeTime + "% faster)");
		System.out.println(">> StackListAllocator (pre-allocated stacks): " + stackListAllocatorReservedTime / MS_TO_SECONDS + " seconds (" + stackListAllocatorReservedRelativeTime + "% faster)");
		System.out.println(">> ListAllocator: " + listAllocatorTime / MS_TO_SECONDS + " seconds (" + listAllocatorRelativeTime + "% faster)");
		System.out.println(">> ArrayAllocator: " + arrayAllocatorTime / MS_TO_SECONDS + " seconds (" + arrayAllocatorRelativeTime + "% faster)");
	}

	private static void warmUp() throws InterruptedException {
		final int n = NUM_ELEMENTS / 10_000;
		final long ustackAllocatorTime = benchmark(n, StackAllocators.unmanagedStackAllocator(TestSchema.SIZE, n, TestSchema.class));
		final long stackAllocatorTime = benchmark(n, StackAllocators.managedStackAllocator(TestSchema.SIZE, n, TestSchema.class));
		final long stackListAllocatorTime = benchmark(n, new StackListAllocator<>(n / 10, TestSchema.SIZE, TestSchema.class, StackAllocators::managedStackAllocator));
		final long stackListAllocatorReservedTime = benchmark(n, new StackListAllocator<>(10, n / 10, TestSchema.SIZE, TestSchema.class, StackAllocators::managedStackAllocator));
		final long arrayAllocatorTime = benchmark(n, new ArrayAllocator<>(10, n / 10, TestSchema.SIZE, TestSchema.class));
		final long listAllocatorTime = benchmark(n, new ListAllocator<>(n / 10, TestSchema.SIZE, TestSchema.class));
		final long defaultAllocatorTime = benchmark(n, new DefaultAllocator<>(TestSchema.SIZE, TestSchema.class));
	}

	private static long benchmark(SchemaAllocator<TestSchema> allocator) throws InterruptedException {
		return benchmark(NUM_ELEMENTS, allocator);
	}

	private static long benchmark(int n, SchemaAllocator<TestSchema> allocator) throws InterruptedException {

		System.out.println("Testing Allocator " + allocator.getClass().getSimpleName());

		Runtime.getRuntime().gc();

		Thread.sleep(100);

		final long startTime = System.currentTimeMillis();

		Schema schema = null;

		for (int i = 0; i < n; i++) {
			schema = allocator.malloc();
		}

		final long time = System.currentTimeMillis() - startTime;

		if (schema != null) {
			System.setProperty("yyy", String.valueOf(schema.address()));
		}

		allocator.free();

		return time;
	}

}

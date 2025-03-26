package systems.intino.test.allocators;

import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.DefaultAllocator;
import systems.intino.datamarts.led.allocators.SchemaAllocator;
import systems.intino.datamarts.led.allocators.stack.StackAllocators;
import systems.intino.datamarts.led.allocators.stack.StackListAllocator;
import systems.intino.test.schemas.TestSchema;
import org.junit.Ignore;

@Ignore
public class TestMemoryUsedByJVM {
	private static final int NUM_ELEMENTS = 20_000_000;
	private static final Runtime RUNTIME = Runtime.getRuntime();
	private static final float BYTES_TO_MB = 1024.0f * 1024.0f;

	public static void main(String[] args) {
		// testDefaultAllocator();
		testStackAllocator();
		// testStackListAllocator();

		//final long defaultAllocatorMemory = getMemoryUsed(new DefaultAllocator<>(TestSchemaObj.SIZE, TestSchemaObj::new));

		//final long stackAllocatorMemory = getMemoryUsed(StackAllocators.newManaged(TestSchemaObj.SIZE, NUM_ELEMENTS, TestSchemaObj::new));

		//final long stackListAllocatorMemory = getMemoryUsed(new StackListAllocator<>(NUM_ELEMENTS / 10, TestSchemaObj.SIZE, TestSchemaObj::new, StackAllocators::newManaged));


		//final float max = Math.max(Math.max(defaultAllocatorMemory, stackAllocatorMemory), stackListAllocatorMemory);

		//final float defaultAllocatorRelativeUsage = defaultAllocatorMemory / max * 100.0f;
		//final float stackAllocatorRelativeUsage = stackAllocatorMemory / max * 100.0f;
		//final float stackListAllocatorRelativeUsage = stackListAllocatorMemory / max * 100.0f;

		//System.out.println("Test Allocator Memory Used (" + NUM_ELEMENTS + " elements, element size in bytes = " + TestSchemaObj.SIZE + ")");
		//System.out.println(">> Off-Heap memory used: " + (NUM_ELEMENTS * TestSchemaObj.SIZE) / BYTES_TO_MB + " MB");
		//System.out.println();
		//System.out.println(">> DefaultAllocator: " + defaultAllocatorMemory / BYTES_TO_MB + " MB used (" + defaultAllocatorRelativeUsage + "%)");
		//System.out.println(">> StackAllocator: " + stackAllocatorMemory / BYTES_TO_MB + " MB used (" + stackAllocatorRelativeUsage + "%)");
		//System.out.println(">> StackListAllocator: " + stackListAllocatorMemory / BYTES_TO_MB + " MB used (" + stackListAllocatorRelativeUsage + "%)");
	}

	private static void testDefaultAllocator() {
		final long defaultAllocatorMemory = getMemoryUsed(new DefaultAllocator<>(TestSchema.SIZE, TestSchema.class));
		System.out.println("Test Allocator Memory Used (" + NUM_ELEMENTS + " elements, element size in bytes = " + TestSchema.SIZE + ")");
		System.out.println(">> Off-Heap memory used: " + (NUM_ELEMENTS * TestSchema.SIZE) / BYTES_TO_MB + " MB");
		System.out.println(">> DefaultAllocator: " + defaultAllocatorMemory / BYTES_TO_MB + " MB used");
	}

	private static void testStackAllocator() {
		final long stackAllocatorMemory = getMemoryUsed(StackAllocators.managedStackAllocator(TestSchema.SIZE, NUM_ELEMENTS, TestSchema.class));
		System.out.println("Test Allocator Memory Used (" + NUM_ELEMENTS + " elements, element size in bytes = " + TestSchema.SIZE + ")");
		System.out.println(">> Off-Heap memory used: " + (NUM_ELEMENTS * TestSchema.SIZE) / BYTES_TO_MB + " MB");
		System.out.println(">> StackAllocator: " + stackAllocatorMemory / BYTES_TO_MB + " MB used");
	}

	private static void testStackListAllocator() {
		final long stackListAllocatorMemory = getMemoryUsed(new StackListAllocator<>(NUM_ELEMENTS / 10, TestSchema.SIZE, TestSchema.class, StackAllocators::managedStackAllocator));
		System.out.println("Test Allocator Memory Used (" + NUM_ELEMENTS + " elements, element size in bytes = " + TestSchema.SIZE + ")");
		System.out.println(">> Off-Heap memory used: " + (NUM_ELEMENTS * TestSchema.SIZE) / BYTES_TO_MB + " MB");
		System.out.println(">> StackListAllocator: " + stackListAllocatorMemory / BYTES_TO_MB + " MB used");
	}

	private static long getMemoryUsed(SchemaAllocator<TestSchema> allocator) {
		RUNTIME.gc();
		final long startMemory = usedMemory();
		Schema schema = null;
		for (int i = 0; i < NUM_ELEMENTS; i++) schema = allocator.malloc();
		if (schema != null) schema.address();
		final long usedMemory = usedMemory() - startMemory;
		allocator.free();
		RUNTIME.gc();
		return usedMemory;
	}

	private static long usedMemory() {
		return RUNTIME.totalMemory() - RUNTIME.freeMemory();
	}
}

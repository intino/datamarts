package systems.intino.test;

import systems.intino.datamarts.led.Led;
import systems.intino.datamarts.led.LedWriter;
import systems.intino.test.schemas.TestSchema;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Random;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class LedBuilder_ {

	private static final File tempFile = new File("temp/snappy_test.led");
	private static final int NUM_ELEMENTS = 1_000_000;

	@BeforeClass
	public static void beforeClass() throws Exception {
		tempFile.getParentFile().mkdirs();
	}

	@AfterClass
	public static void afterClass() throws Exception {
		tempFile.delete();
	}

	@Test
	public void should_build_led() {
		Led<TestSchema> led = buildLed();
		for(TestSchema schema : led) {
			assertNotNull(schema);
		}
	}

	@Test
	public void should_build_and_write_led() {
		Led<TestSchema> led = buildLed();
		new LedWriter(tempFile).write(led);
	}

	@Test
	public void should_be_sorted() {
		Led<TestSchema> led = buildLed();
		long lastId = Long.MIN_VALUE;
		for(int i = 0;i < led.size();i++) {
			final long id = led.schema(i).id();
			assertTrue(id >= lastId);
			lastId = id;
		}
	}

	private Led<TestSchema> buildLed() {
		Led.Builder<TestSchema> builder = Led.builder(TestSchema.class);
		Random random = new Random();
		for (int i = 0; i < NUM_ELEMENTS; i++) {
			final int id = random.nextInt();
			final int index = i;
			builder.create(t -> t.id(id).b(index - 500).f(index * 100.0 / 20.0));
		}
		return builder.build();
	}

	@After
	public void tearDown() {
		tempFile.delete();
		tempFile.getParentFile().delete();
	}
}

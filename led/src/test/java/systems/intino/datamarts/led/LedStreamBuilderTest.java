package systems.intino.datamarts.led;

import io.intino.alexandria.logger.Logger;
import org.junit.Test;
import systems.intino.test.schemas.TestSchema;

import java.io.File;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;


public class LedStreamBuilderTest {

    public static final int NUM_ELEMENTS = 5_000_000;

    @Test
    public void test() {
        System.out.println(TestSchema.SIZE);
        LedStream.Builder<TestSchema> builder = LedStream.builder(TestSchema.class, 100_000, new File("temp"));
        Random random = new Random();
        double start = System.currentTimeMillis();
        for(int i = 0; i < NUM_ELEMENTS; i++) {
            long id = i;
            builder.append(t -> t.id(random.nextInt(Integer.MAX_VALUE / 10)));
            if(i % 1_000_000 == 0) {
                double time = (System.currentTimeMillis() - start) / 1000.0;
                System.out.println(">> Created " + i + " elements (" + time + " seconds)");
            }
        }

        System.out.println("Build finish");

        start = System.currentTimeMillis();

        try(LedStream<TestSchema> ledStream = builder.build()) {

            AtomicLong lastId = new AtomicLong(Long.MIN_VALUE);
            AtomicInteger i = new AtomicInteger();

            ledStream.peek(item -> {
                final long id = item.id();
                assertTrue(i.get() + " => " + id + " < " + lastId, lastId.get() <= id);
                lastId.set(id);
                i.incrementAndGet();
            }).serialize(new File("temp/ledstreambuilder_full_led_" + NUM_ELEMENTS +".led"));

        } catch (Exception e) {
            Logger.error(e);
        }

        double time = (System.currentTimeMillis() - start) / 1000.0;

        System.out.println("iteration in " + time + " seconds");

    }

}
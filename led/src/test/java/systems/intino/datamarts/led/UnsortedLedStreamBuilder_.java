package systems.intino.datamarts.led;

import io.intino.alexandria.logger.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Random;

public class UnsortedLedStreamBuilder_ {

    public static final int NUM_ELEMENTS = 10_000_000;
    public static final File BUILDER_FILE = new File("temp/builder_led" + NUM_ELEMENTS + ".led");
    public static final File FINAL_LED_FILE = new File("temp/led" + NUM_ELEMENTS + ".led");
    private static final Random RANDOM = new Random(System.nanoTime());

    static {
        BUILDER_FILE.getParentFile().mkdirs();
    }

    @Test
    public void test() {
        System.out.println(Item.SIZE);
        LedStream.Builder<Item> builder = new UnsortedLedStreamBuilder<>(Item.class, BUILDER_FILE);

        double start = System.currentTimeMillis();
        createItems(builder, start);
        start = System.currentTimeMillis();

        try(LedStream<Item> ledStream = builder.build()) {
            //testLeds(ledStream);
            //ledStream.serialize(FINAL_LED_FILE);
        } catch (Exception e) {
            Logger.error(e);
        }

        double time = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("iteration in " + time + " seconds");

        //testLedFile();
    }

    private void testLedFile() {
        testLeds(new LedReader(FINAL_LED_FILE).read(Item.class));
    }

    private void testLeds(LedStream<Item> items) {
        long index = 0;
        while(items.hasNext()) {
            Item item = items.next();
            Assert.assertEquals(index, item.id());
            Assert.assertEquals((int)index, item.a());
            Assert.assertEquals((float)index, item.b(), 0.001f);
            Assert.assertEquals((short)index, item.c());
            Assert.assertEquals((short)index, item.d());
            ++index;
        }
    }

    private void createItems(LedStream.Builder<Item> builder, double start) {
        for(int i = 0;i < NUM_ELEMENTS;i++) {
            final long index = i;

            builder.append(t -> {
                t.id(index);
                t.a((int)index);
                t.b((float)index);
                t.c((short)index);
                t.d((short)index);
            });


            if(i % (NUM_ELEMENTS/10) == 0) {
                double time = (System.currentTimeMillis() - start) / 1000.0;
                System.out.println(">> Created " + i + " elements (" + time + " seconds)");
            }
        }
        System.out.println("Build finish");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        BUILDER_FILE.delete();
    }


}
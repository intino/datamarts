package systems.intino.datamarts.led;

import systems.intino.datamarts.led.util.sorting.LedExternalMergeSort;
import io.intino.alexandria.logger.Logger;
import org.junit.*;

import java.io.File;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class TestLedExternalMergeSort {

    private static final int NUM_SCHEMAS_IN_MEMORY = 100_000;

    private static final File SRC = new File("temp/unsorted_led.led");
    private static final File DST = new File("temp/sorted_led.led");

    @BeforeClass
    public static void beforeClass() throws Exception {
        SRC.delete();
        DST.delete();
    }

    @Before
    public void setUp() throws Exception {
        SRC.getParentFile().mkdirs();
        DST.getParentFile().mkdirs();
    }

    @After
    public void tearDown() throws Exception {
        SRC.delete();
        DST.delete();
    }

    @Test
    public void testEmpty() {
        createLed(SRC, 0);
        mergeSort(0);
    }

    @Test
    public void testOne() {
        createLed(SRC, 1);
        mergeSort(1);
    }

    @Test
    public void testFew() {
        final int numTransactions = 11;
        createLed(SRC, numTransactions);
        mergeSort(numTransactions);
    }

    @Test
    public void testNormal() {
        final int numTransactions = 2_501_017;
        createLed(SRC, numTransactions);
        mergeSort(numTransactions);
    }

    @Ignore
    @Test
    public void testLarge() {
        final int numTransactions = 10_486_926;
        createLed(SRC, numTransactions);
        mergeSort(numTransactions);
    }

    @Ignore
    @Test
    public void testSuperLarge() {
        final int numTransactions = 50_000_001;
        createLed(SRC, numTransactions);
        mergeSort(numTransactions);
    }

    @Ignore
    @Test
    public void testMegaLarge() {
        final int numTransactions = 100_000_001;
        createLed(SRC, numTransactions);
        mergeSort(numTransactions);
    }

    private void mergeSort(int numTransactions) {
        System.out.println(">> Testing " + SRC + "(" + SRC.length() / 1024.0 / 1024.0 + " MB)...");
        LedHeader sourceHeader = LedHeader.from(SRC);

        doSort();

        System.out.println("	>> Validating result led...");
        LedHeader destHeader = LedHeader.from(DST);
        assertEquals("Sorting did not maintain information: " + sourceHeader.elementCount() + " != " + destHeader.elementCount(),
                sourceHeader.elementCount(),
                destHeader.elementCount());

        checkData(numTransactions);
    }

    private void checkData(int numTransactions) {
        System.out.println(">> Checking data...");

        try(LedStream<Item> ledStream = new LedReader(DST).read(Item.class)) {

            long index = 0;
            while(ledStream.hasNext()) {
                Item item = ledStream.next();
                assertEquals(index, item.id());
                assertEquals((int)index, item.a());
                assertEquals((float)index, item.b(), 0.001f);
                assertEquals((short)index, item.c());
                assertEquals((short)index, item.d());
                ++index;
            }

        } catch (Exception e) {
            Logger.error(e);
        }
        System.out.println(">> Data validation OK");
    }

    private void doSort() {
        new LedExternalMergeSort(SRC, DST)
                .numTransactionsInMemory(NUM_SCHEMAS_IN_MEMORY)
                .checkChunkSorting(true)
                .sort();
    }

    private void createLed(File file, int numTransactions) {
        System.out.println("Creating unsorted led of " + numTransactions + ", each schema of " + Item.SIZE + " bytes...");
        LedStream.Builder<Item> builder = new UnsortedLedStreamBuilder<>(Item.class, file);
        Random random = new Random();
        double start = System.currentTimeMillis();
        for(int i = numTransactions - 1;i >= 0;i--) {
            final long id = i;
            builder.append(t -> t.id(id).a((int)id).b((float)id).c((short)id).d((short)id));
            if(i % 1_000_000 == 0) {
                double time = (System.currentTimeMillis() - start) / 1000.0;
                System.out.println(">> Created " + i + " elements (" + time + " seconds)");
            }
        }
        builder.build();
        System.out.println("Build finish");
    }
}

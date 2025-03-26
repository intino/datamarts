package systems.intino.concurrency;

import systems.intino.datamarts.led.LedStream;
import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.UnsortedLedStreamBuilder;
import systems.intino.datamarts.led.allocators.SchemaFactory;
import systems.intino.datamarts.led.buffers.store.ByteStore;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Ignore
public class UnsortedLedStreamBuilderMultithreading {

    private static final int NUM_THREADS = 8;
    private static final long NUM_TRANSACTIONS = 10_000_000;

    @Test
    public void testMultipleThreads() throws InterruptedException {
        System.out.println("Testing multiple threads...");

        final File tempFile = new File("test_unsorted.led");
        tempFile.delete();
        UnsortedLedStreamBuilder<Transaction> builder = new UnsortedLedStreamBuilder<>(Transaction.class, tempFile);
        ExecutorService threadPool = Executors.newCachedThreadPool();

        final double start = System.currentTimeMillis();
        for(long i = 1;i <= NUM_TRANSACTIONS;i++) {
            final long index = i;
            threadPool.submit(() -> builder.append(t -> t.id(index)));
        }
        threadPool.shutdown();
        threadPool.awaitTermination(10, TimeUnit.DAYS);

        final double end = System.currentTimeMillis();
        System.out.println("Time multiple threads = " + (end - start) / 1000.0 + " seconds");

        LedStream<Transaction> stream = builder.build();
        Set<Long> ids = new LinkedHashSet<>();

        stream.forEach(t -> {
            Assert.assertTrue("Id out of range: " + t.id(), t.id() > 0 && t.id() <= NUM_TRANSACTIONS);
            Assert.assertTrue("Repeated id: " + t.id(), ids.add(t.id()));
        });
    }

    @Test
    public void testSingleThread() {
        System.out.println("Testing single thread...");

        final File tempFile = new File("test_unsorted.led");
        tempFile.delete();
        UnsortedLedStreamBuilder<Transaction> builder = new UnsortedLedStreamBuilder<>(Transaction.class, tempFile);

        final double start = System.currentTimeMillis();
        for(long i = 1;i <= NUM_TRANSACTIONS;i++) {
            final long index = i;
            builder.append(t -> t.id(index));
        }
        final double end = System.currentTimeMillis();
        System.out.println("Time single thread = " + (end - start) / 1000.0 + " seconds");

        LedStream<Transaction> stream = builder.build();
        Set<Long> ids = new LinkedHashSet<>();
        stream.forEach(t -> {
            Assert.assertTrue("Id out of range: " + t.id(), t.id() > 0 && t.id() <= NUM_TRANSACTIONS);
            Assert.assertTrue("Repeated id: " + t.id(), ids.add(t.id()));
        });
    }

    private static class Transaction extends Schema {

        public static final int SIZE = Long.BYTES;
        public static final UUID SERIAL_UUID = UUID.randomUUID();
        public static final SchemaFactory<Transaction> FACTORY = new SchemaFactory<>(Transaction.class) {
            @Override
            public Transaction newInstance(ByteStore store) {
                return new Transaction(store);
            }
        };

        public Transaction(ByteStore store) {
            super(store);
        }

        @Override
        public long id() {
            return bitBuffer.getAlignedLong(0);
        }

        public Transaction id(long id) {
            bitBuffer.setAlignedLong(0, id);
            return this;
        }

        @Override
        public int size() {
            return SIZE;
        }

        @Override
        public UUID serialUUID() {
            return SERIAL_UUID;
        }
    }
}

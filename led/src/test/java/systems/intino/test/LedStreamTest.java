package systems.intino.test;

import systems.intino.datamarts.led.LedStream;
import systems.intino.datamarts.led.allocators.SchemaAllocator;
import systems.intino.datamarts.led.allocators.stack.StackAllocators;
import systems.intino.datamarts.led.leds.IteratorLedStream;
import systems.intino.test.schemas.TestSchema;
import systems.intino.test.schemas.Venta;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class LedStreamTest {

    private LedStream<TestSchema> ledStream;
    private final int numElements;

    public LedStreamTest() {
        numElements = 521_331;
    }

    @Before
    public void setUp()  {
        ledStream = createTestTransactionsLedStream(Function.identity());
    }

    private LedStream<TestSchema> createTestTransactionsLedStream(Function<Integer, Integer> indexMapper) {
        SchemaAllocator<TestSchema> allocator = StackAllocators.managedStackAllocator(TestSchema.SIZE, numElements, TestSchema.class);
        return IteratorLedStream.fromStream(TestSchema.class,
                IntStream.range(1, numElements).map(indexMapper::apply).mapToObj(i ->
                        allocator.calloc().id(i).a((short) (i + 1)).b(i * 2)));
    }

    @Test
    public void testFilter() {
        Iterator<TestSchema> TestTransactions = ledStream.filter(c -> c.id() % 2 == 0);
        while(TestTransactions.hasNext()) {
            assertEquals(0, TestTransactions.next().id() % 2);
        }
    }

    @Test
    public void testPeek() {
        Iterator<TestSchema> TestTransactions = ledStream.peek(c -> c.id(c.id() * -1));
        int i = 1;
        while(TestTransactions.hasNext()) {
            assertEquals(-i, TestTransactions.next().id());
            ++i;
        }
    }

    @Test
    public void testMap() {

        Queue<TestSchema> testTransactions = new ArrayDeque<>(numElements);

        Iterator<Venta> ventas = ledStream.map(
                Venta.SIZE, Venta.class, (c, ca) -> {
                    ca.id(c.id()).kwh(c.a()).importe(c.b());
                    testTransactions.add(c);
                });

        while(ventas.hasNext()) {
            Venta venta = ventas.next();
            TestSchema testTransaction = testTransactions.poll();
            assertEquals(testTransaction.id(), venta.id());
            assertEquals(testTransaction.a(),venta.kwh(), 0.0001f);
            assertEquals(testTransaction.b(),venta.importe(), 0.0001f);
        }
    }

    @Test
    public void testMerge() {

        Queue<TestSchema> TestTransactionsQueue = new PriorityQueue<>();

        LedStream<TestSchema> a = createTestTransactionsLedStream(Function.identity()).peek(TestTransactionsQueue::add);
        LedStream<TestSchema> b = createTestTransactionsLedStream(i -> i + numElements).peek(TestTransactionsQueue::add);

        LedStream<TestSchema> merge = a.merge(b);

        while(merge.hasNext()) {
            TestSchema actual = merge.next();
            TestSchema expected = TestTransactionsQueue.poll();
            assertEquals(expected.id(), actual.id());
        }
    }

    @Test
    public void testRemoveAllNoRemoving() {

        Queue<TestSchema> TestTransactions = new ArrayDeque<>(numElements);

        LedStream<TestSchema> a = createTestTransactionsLedStream(Function.identity()).peek(TestTransactions::add);
        LedStream<TestSchema> b = createTestTransactionsLedStream(i -> i + numElements);

        LedStream<TestSchema> complementAB = a.removeAll(b);

        assertTrue(complementAB.hasNext());

        while(complementAB.hasNext()) {
            TestSchema actual = complementAB.next();
            TestSchema expected = TestTransactions.poll();
            assertEquals(expected.id(), actual.id());
        }
    }

    @Test
    public void testRemoveAllResultEmpty() {

        LedStream<TestSchema> a = createTestTransactionsLedStream(Function.identity());
        LedStream<TestSchema> b = createTestTransactionsLedStream(Function.identity());

        LedStream<TestSchema> complementAB = a.removeAll(b);

        assertFalse(complementAB.hasNext());
    }

    @Test
    public void testRemoveAllEvenids() {

        Queue<TestSchema> TestTransactionsOddid = new ArrayDeque<>(numElements);

        LedStream<TestSchema> a = createTestTransactionsLedStream(Function.identity()).peek(c -> {
            if(c.id() % 2 != 0) {
                TestTransactionsOddid.add(c);
            }
        });
        LedStream<TestSchema> b = createTestTransactionsLedStream(i -> i * 2);

        LedStream<TestSchema> complementAB = a.removeAll(b);

        assertTrue(complementAB.hasNext());

        while(complementAB.hasNext()) {
            TestSchema actual = complementAB.next();
            assertNotEquals(0, actual.id() % 2);
            TestSchema expected = TestTransactionsOddid.poll();
            assertEquals(expected.id(), actual.id());
        }
    }

    @Test
    public void testRetainAllEmptyResult() {
        LedStream<TestSchema> a = createTestTransactionsLedStream(Function.identity());
        LedStream<TestSchema> b = createTestTransactionsLedStream(i -> -i);

        LedStream<TestSchema> intersectionAB = a.retainAll(b);

        assertFalse(intersectionAB.hasNext());
    }

    @Test
    public void testRetainAllSameLedStream() {

        Queue<TestSchema> TestTransactions = new ArrayDeque<>(numElements);

        LedStream<TestSchema> a = createTestTransactionsLedStream(Function.identity()).peek(TestTransactions::add);
        LedStream<TestSchema> b = createTestTransactionsLedStream(Function.identity());

        LedStream<TestSchema> intersectionAB = a.retainAll(b);

        assertTrue(intersectionAB.hasNext());

        while(intersectionAB.hasNext()) {
            TestSchema actual = intersectionAB.next();
            TestSchema expected = TestTransactions.poll();
            assertEquals(expected.id(), actual.id());
        }
    }

    @Test
    public void testRetainAllEvenids() {

        Queue<TestSchema> TestTransactionsEvenid = new ArrayDeque<>(numElements);

        LedStream<TestSchema> a = createTestTransactionsLedStream(Function.identity()).peek(c -> {
            if(c.id() % 2 == 0) {
                TestTransactionsEvenid.add(c);
            }
        });
        LedStream<TestSchema> b = createTestTransactionsLedStream(i -> i * 2);

        LedStream<TestSchema> intersectionAB = a.retainAll(b);

        assertTrue(intersectionAB.hasNext());

        while(intersectionAB.hasNext()) {
            TestSchema actual = intersectionAB.next();
            assertEquals(0, actual.id() % 2);
            TestSchema expected = TestTransactionsEvenid.poll();
            assertEquals(expected.id(), actual.id());
        }
    }
}
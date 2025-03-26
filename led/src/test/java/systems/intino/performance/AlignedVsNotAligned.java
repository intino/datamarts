package systems.intino.performance;

import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.SchemaFactory;
import systems.intino.datamarts.led.buffers.store.ByteBufferStore;
import systems.intino.datamarts.led.buffers.store.ByteStore;
import org.junit.Ignore;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@Ignore
public class AlignedVsNotAligned {

    static {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static final int N = 100_000_000;
    private static double x = Math.random();


    private static List<String> list;

    public static void main(String[] args) {
        setUp();
        testAligned();
        testNotAligned();
    }

    public static void setUp() {
        list = new LinkedList<>();
        for(int i = 0;i < N/10;i++) {
            list.add(String.valueOf(Math.log(i+1)));
        }
        x *= new Random().nextFloat();
    }

    public static void testAligned() {
        Aligned aligned = new Aligned();
        System.out.println("Aligned size = " + aligned.size() + " bytes");
        double start = System.currentTimeMillis();
        for(int i = 0;i < N;i++) {
            aligned.id(i);
            aligned.a((int) (aligned.id() + i));
            aligned.b((short) (aligned.a() + i));
            aligned.c((byte) (aligned.b() + i));
            x += aligned.c();
        }
        double end = System.currentTimeMillis();
        list.add(String.valueOf(x));
        System.out.println(list.get(new Random().nextInt(list.size())));
        System.out.println("Time Aligned: " + (end - start) / 1000.0f + " seconds");
    }

    public static void testNotAligned() {
        NotAligned notAligned = new NotAligned();
        System.out.println("NotAligned size = " + notAligned.size() + " bytes");
        double start = System.currentTimeMillis();
        for(int i = 0;i < N;i++) {
            notAligned.id(i);
            notAligned.a((int) (notAligned.id() + i));
            notAligned.b((short) (notAligned.a() + i));
            notAligned.c((byte) (notAligned.b() + i));
            x += notAligned.c();
        }
        double end = System.currentTimeMillis();
        list.add(String.valueOf(x));
        System.out.println(list.get(new Random().nextInt(list.size())));
        System.out.println("Time NOT Aligned: " + (end - start) / 1000.0f + " seconds");
    }

    private static class Aligned extends Schema {

        private static final int OFFSET1 = 0;
        private static final int SIZE1 = Long.SIZE;
        private static final int OFFSET2 = OFFSET1 + SIZE1;
        private static final int SIZE2 = Integer.SIZE;
        private static final int OFFSET3 = OFFSET2 + SIZE2;
        private static final int SIZE3 = Short.SIZE;
        private static final int OFFSET4 = OFFSET3 + SIZE3;
        private static final int SIZE4 = Byte.SIZE;

        public static final int SIZE = (int)Math.ceil((OFFSET4 + SIZE4) /(float) 8);

        public static final SchemaFactory<Aligned> FACTORY = new SchemaFactory<>(Aligned.class) {
            @Override
            public Aligned newInstance(ByteStore store) {
                return new Aligned(store);
            }
        };

        public Aligned() {
            this(new ByteBufferStore(SIZE));
        }

        public Aligned(ByteStore store) {
            super(store);
        }

        @Override
        public long id() {
            return bitBuffer.getAlignedLong(OFFSET1);
        }

        public void id(long id) {
            bitBuffer.setAlignedLong(OFFSET1, id);
        }

        public int a() {
            return bitBuffer.getAlignedInteger(OFFSET2);
        }

        public void a(int x) {
            bitBuffer.setAlignedInteger(OFFSET2, x);
        }

        public short b() {
            return bitBuffer.getAlignedShort(OFFSET3);
        }

        public void b(short x) {
            bitBuffer.setAlignedShort(OFFSET3, x);
        }

        public byte c() {
            return bitBuffer.getAlignedByte(OFFSET4);
        }

        public void c(byte x) {
            bitBuffer.setAlignedByte(OFFSET4, x);
        }

        @Override
        public int size() {
            return SIZE;
        }

        @Override
        public UUID serialUUID() {
            return null;
        }
    }

    private static class NotAligned extends Schema {

        private static final int OFFSET1 = 0;
        private static final int SIZE1 = Long.SIZE;
        private static final int OFFSET2 = OFFSET1 + SIZE1;
        private static final int SIZE2 = 18;
        private static final int OFFSET3 = OFFSET2 + SIZE2;
        private static final int SIZE3 = 12;
        private static final int OFFSET4 = OFFSET3 + SIZE3;
        private static final int SIZE4 = 3;

        public static final int SIZE = (int)Math.ceil((OFFSET4 + SIZE4) /(float) 8);

        public static final SchemaFactory<NotAligned> FACTORY = new SchemaFactory<>(NotAligned.class) {
            @Override
            public NotAligned newInstance(ByteStore store) {
                return new NotAligned(store);
            }
        };

        public NotAligned() {
            this(new ByteBufferStore(SIZE));
        }

        public NotAligned(ByteStore store) {
            super(store);
        }

        @Override
        public long id() {
            return bitBuffer.getAlignedLong(OFFSET1);
        }

        public void id(long id) {
            bitBuffer.setAlignedLong(OFFSET1, id);
        }

        public int a() {
            return bitBuffer.getAlignedInteger(OFFSET2);
        }

        public void a(int x) {
            bitBuffer.setAlignedInteger(OFFSET2, x);
        }

        public short b() {
            return bitBuffer.getShortNBits(OFFSET3, SIZE3);
        }

        public void b(short x) {
            bitBuffer.setShortNBits(OFFSET3, SIZE3, x);
        }

        public byte c() {
            return bitBuffer.getByteNBits(OFFSET4, SIZE4);
        }

        public void c(byte x) {
            bitBuffer.setByteNBits(OFFSET4, SIZE4, x);
        }

        @Override
        public int size() {
            return SIZE;
        }

        @Override
        public UUID serialUUID() {
            return null;
        }
    }
}

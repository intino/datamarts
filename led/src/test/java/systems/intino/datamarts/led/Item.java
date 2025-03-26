package systems.intino.datamarts.led;

import systems.intino.datamarts.led.allocators.DefaultAllocator;
import systems.intino.datamarts.led.allocators.SchemaAllocator;
import systems.intino.datamarts.led.allocators.SchemaFactory;
import systems.intino.datamarts.led.buffers.store.ByteStore;

import java.util.UUID;

public class Item extends Schema {

    public static void main(String[] args) {
        SchemaAllocator<Item> allocator = new DefaultAllocator<>(Item.SIZE, Item.class);
        Item item = allocator.malloc();
    }

    public static final int ID_OFFSET = 0;
    public static final int ID_SIZE = Long.SIZE;
    public static final int A_OFFSET = ID_SIZE;
    public static final int A_SIZE = Integer.SIZE;
    public static final int B_OFFSET = A_OFFSET + A_SIZE;
    public static final int B_SIZE = Float.SIZE;
    public static final int C_OFFSET = B_OFFSET + B_SIZE;
    public static final int C_SIZE = Short.SIZE;
    public static final int D_OFFSET = C_OFFSET + C_SIZE;
    public static final int D_SIZE = Short.SIZE;

    public static final int SIZE = (int) Math.ceil((D_OFFSET + D_SIZE) / 8.0);
    public static final UUID SERIAL_UUID = UUID.nameUUIDFromBytes("Item".getBytes());
    public static SchemaFactory<Item> FACTORY = new SchemaFactory<>(Item.class) {
        @Override
        public Item newInstance(ByteStore store) {
            return new Item(store);
        }
    };

    public Item(ByteStore store) {
        super(store);
    }

    @Override
    public long id() {
        return bitBuffer.getAlignedLong(ID_OFFSET);
    }

    public Item id(long id) {
        bitBuffer.setAlignedLong(ID_OFFSET, id);
        return this;
    }

    public int a() {
        return bitBuffer.getAlignedInteger(A_OFFSET);
    }

    public Item a(int a) {
        bitBuffer.setAlignedInteger(A_OFFSET, a);
        return this;
    }

    public float b() {
        return bitBuffer.getAlignedReal32Bits(B_OFFSET);
    }

    public Item b(float b) {
        bitBuffer.setAlignedReal32Bits(B_OFFSET, b);
        return this;
    }

    public short c() {
        return bitBuffer.getAlignedShort(C_OFFSET);
    }

    public Item c(short c) {
        bitBuffer.setAlignedShort(C_OFFSET, c);
        return this;
    }

    public short d() {
        return bitBuffer.getAlignedShort(D_OFFSET);
    }

    public Item d(short d) {
        bitBuffer.setAlignedShort(D_OFFSET, d);
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

    @Override
    public String toString() {
        return "Item{" +
                "id=" + id() +
                ", a=" + a() +
                ", b=" + b() +
                ", c=" + c() +
                ", d=" + d() +
                '}';
    }
}

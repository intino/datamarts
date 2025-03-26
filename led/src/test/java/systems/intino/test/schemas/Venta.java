package systems.intino.test.schemas;

import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.allocators.SchemaFactory;
import systems.intino.datamarts.led.buffers.store.ByteBufferStore;
import systems.intino.datamarts.led.buffers.store.ByteStore;
import systems.intino.datamarts.led.util.memory.MemoryAddress;
import systems.intino.datamarts.led.util.memory.MemoryUtils;

import java.nio.ByteBuffer;
import java.util.UUID;

public class Venta extends Schema {

    public static final int SIZE = (int)Math.ceil(16.25D);
    public static final UUID SERIAL_UUID = UUID.nameUUIDFromBytes(Venta.class.getName().getBytes());
    public static final SchemaFactory<Venta> FACTORY = new SchemaFactory<>(Venta.class) {
        @Override
        public Venta newInstance(ByteStore store) {
            return new Venta(store);
        }
    };

    public Venta() {
        super(defaultByteStore());
    }

    public Venta(ByteStore store) {
        super(store);
    }

    public int size() {
        return SIZE;
    }

    @Override
    public UUID serialUUID() {
        return SERIAL_UUID;
    }

    public long id() {
        return this.bitBuffer.getAlignedLong(0);
    }

    public Venta id(long id) {
        this.bitBuffer.setAlignedLong(0, id);
        return this;
    }

    public Integer importe() {
        return this.bitBuffer.getAlignedInteger(64);
    }

    public Integer diasFacturados() {
        return this.bitBuffer.getIntegerNBits(96, 16);
    }

    public Integer kwh() {
        return this.bitBuffer.getIntegerNBits(112, 16);
    }

    public short concepto() {
        short value = this.bitBuffer.getShortNBits(128, 2);
        return value;
    }

    public Venta importe(int importe) {
        this.bitBuffer.setAlignedInteger(64, importe);
        return this;
    }

    public Venta diasFacturados(int diasFacturados) {
        this.bitBuffer.setIntegerNBits(96, 16, diasFacturados);
        return this;
    }

    public Venta kwh(int kwh) {
        this.bitBuffer.setIntegerNBits(112, 16, kwh);
        return this;
    }

    public Venta concepto(short concepto) {
        this.bitBuffer.setIntegerNBits(128, 2, concepto);
        return this;
    }

    private static ByteStore defaultByteStore() {
        ByteBuffer buffer = MemoryUtils.allocBuffer(130L);
        MemoryAddress address = MemoryAddress.of(buffer);
        return new ByteBufferStore(buffer, address, 0, buffer.capacity());
    }

}

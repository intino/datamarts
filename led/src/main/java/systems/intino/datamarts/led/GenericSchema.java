package systems.intino.datamarts.led;

import systems.intino.datamarts.led.allocators.SchemaFactory;
import systems.intino.datamarts.led.buffers.store.ByteStore;

import java.util.UUID;

public final class GenericSchema extends Schema {

    public static final SchemaFactory<GenericSchema> FACTORY = new SchemaFactory<>(GenericSchema.class) {
        @Override
        public GenericSchema newInstance(ByteStore store) {
            return new GenericSchema(store);
        }
    };

    public GenericSchema(ByteStore store) {
        super(store);
    }

    @Override
    public long id() {
        return bitBuffer.getAlignedLong(0);
    }

    @Override
    public int size() {
        return (int) bitBuffer.byteSize();
    }

    @Override
    public UUID serialUUID() {
        throw new UnsupportedOperationException("Unknown UUID for this schema");
    }

    @Override
    public String toString() {
        return "GenericSchema{" +
                "id=" + id() +
                ", size=" + size() +
                '}';
    }
}

package systems.intino.test.schemas;

import systems.intino.datamarts.led.Schema;
import systems.intino.datamarts.led.buffers.store.ByteBufferStore;
import systems.intino.datamarts.led.buffers.store.ByteStore;
import systems.intino.datamarts.led.util.memory.MemoryAddress;
import systems.intino.datamarts.led.util.memory.MemoryUtils;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class VentaEnergia extends Schema {

    public static void main(String[] args) {

        VentaEnergia ventaEnergia = new VentaEnergia().claseVenta(1).concepto(3).total(12345).ocr(98765);

        System.out.println(ventaEnergia);

    }

    public static final int SIZE = 52;

    public static final UUID SERIAL_UUID = UUID.randomUUID();

    public VentaEnergia() {
        super(defaultByteStore());
    }

    public VentaEnergia(ByteStore store) {
        super(store);
    }

    public int size() {
        return 52;
    }

    @Override
    public UUID serialUUID() {
        return SERIAL_UUID;
    }

    public Map<String, Object> values() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("total", this.total());
        values.put("dap", this.dap());
        values.put("iva", this.iva());
        values.put("importe", this.importe());
        values.put("kwh", this.kwh());
        values.put("ocr", this.ocr());
        values.put("diasFacturados", this.diasFacturados());
        values.put("claseVenta", this.claseVenta());
        values.put("concepto", this.concepto());
        return values;
    }

    @Override
    public long id() {
        return this.ocr();
    }

    public long total() {
        return this.bitBuffer.getAlignedLong(0);
    }

    public long dap() {
        return this.bitBuffer.getAlignedLong(64);
    }

    
    public long iva() {
        return this.bitBuffer.getAlignedLong(128);
    }

    
    public long importe() {
        return this.bitBuffer.getLongNBits(192, 64);
    }

    
    public long kwh() {
        return this.bitBuffer.getAlignedLong(256);
    }

    
    public long ocr() {
        return this.bitBuffer.getLongNBits(320, 64);
    }

    
    public int diasFacturados() {
        return this.bitBuffer.getIntegerNBits(384, 16);
    }

    
    public int claseVenta() {
        return this.bitBuffer.getIntegerNBits(400, 8);
    }

    
    public int concepto() {
        return this.bitBuffer.getIntegerNBits(408, 7);
    }

    public VentaEnergia total(long total) {
        this.bitBuffer.setAlignedLong(0, total);
        return this;
    }

    public VentaEnergia dap(long dap) {
        this.bitBuffer.setAlignedLong(64, dap);
        return this;
    }

    public VentaEnergia iva(long iva) {
        this.bitBuffer.setAlignedLong(128, iva);
        return this;
    }

    public VentaEnergia importe(long importe) {
        this.bitBuffer.setLongNBits(192, 64, importe);
        return this;
    }

    public VentaEnergia kwh(long kwh) {
        this.bitBuffer.setAlignedLong(256, kwh);
        return this;
    }

    public VentaEnergia ocr(long ocr) {
        this.bitBuffer.setLongNBits(320, 64, ocr);
        return this;
    }

    public VentaEnergia diasFacturados(int diasFacturados) {
        this.bitBuffer.setIntegerNBits(384, 16, diasFacturados);
        return this;
    }

    public VentaEnergia claseVenta(int claseVenta) {
        this.bitBuffer.setIntegerNBits(400, 8, claseVenta);
        return this;
    }

    public VentaEnergia concepto(int concepto) {
        this.bitBuffer.setIntegerNBits(408, 7, concepto);
        return this;
    }

    private static ByteStore defaultByteStore() {
        ByteBuffer buffer = MemoryUtils.allocBuffer(52L);
        MemoryAddress address = MemoryAddress.of(buffer);
        return new ByteBufferStore(buffer, address, 0, buffer.capacity());
    }

}

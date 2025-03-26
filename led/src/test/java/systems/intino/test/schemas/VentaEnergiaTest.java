package systems.intino.test.schemas;

import systems.intino.datamarts.led.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class VentaEnergiaTest {

    private VentaEnergia ventaEnergia;

    @Before
    public void setup() {
        ventaEnergia = new VentaEnergia();
    }

    @Test
    public void size() {
        assertEquals(VentaEnergia.SIZE, ventaEnergia.size());
    }

    @Test
    public void id() {
        final long id = 12345;
        ventaEnergia.ocr(id);
        assertEquals(id, Schema.idOf(ventaEnergia));
        assertEquals(id, ventaEnergia.id());
        assertEquals(id, ventaEnergia.ocr());
    }

    @Test
    public void testSetFieldDoesNotChangeOtherFields() {
        List<Function<VentaEnergia, Long>> getters = getters();
        List<BiConsumer<VentaEnergia, Long>> setters = setters();
        long[] lastValues = new long[getters.size()];
        Random random = new Random(System.nanoTime());

        for(int i = 0;i < setters.size();i++) {
            BiConsumer<VentaEnergia, Long> setter = setters.get(i);
            long value = random.nextLong() - random.nextLong();
            setter.accept(ventaEnergia, value);
            // Assert the value is correctly set
            assertEquals(value, (long)getters.get(i).apply(ventaEnergia));
            lastValues[i] = value;
            // Check if other fields have been affected
            for(int j = 0;j < getters.size();j++) {
                if(i == j) continue;
                assertEquals(lastValues[j], (long)getters.get(j).apply(ventaEnergia));
            }
            //System.out.println(ventaEnergia.values());
        }
    }

    private List<BiConsumer<VentaEnergia, Long>> setters() {
        return List.of(
                VentaEnergia::total,
                VentaEnergia::dap,
                VentaEnergia::iva,
                VentaEnergia::importe,
                VentaEnergia::kwh,
                VentaEnergia::ocr);
    }

    private List<Function<VentaEnergia, Long>> getters() {
        return List.of(
                VentaEnergia::total,
                VentaEnergia::dap,
                VentaEnergia::iva,
                VentaEnergia::importe,
                VentaEnergia::kwh,
                VentaEnergia::ocr);
    }

    private void testInt64(Function<VentaEnergia, Long> getter, BiConsumer<VentaEnergia, Long> setter) {
        final long[] values = {Long.MIN_VALUE, Long.MIN_VALUE / 2, -1, 0, 1, Long.MAX_VALUE / 2, Long.MAX_VALUE};
        for(long value : values) {
            setter.accept(ventaEnergia, value);
            assertEquals(value, getter.apply(ventaEnergia).longValue());
        }
        Random random = new Random(System.nanoTime());
        for(int i = 0;i < 1000000;i++) {
            long value = random.nextLong() - random.nextLong();
            setter.accept(ventaEnergia, value);
            assertEquals(value, getter.apply(ventaEnergia).longValue());
        }
    }

    @Test
    public void total() {
        testInt64(VentaEnergia::total, VentaEnergia::total);
    }

    @Test
    public void dap() {
        testInt64(VentaEnergia::dap, VentaEnergia::dap);
    }

    @Test
    public void iva() {
        testInt64(VentaEnergia::iva, VentaEnergia::iva);
    }

    @Test
    public void importe() {
        testInt64(VentaEnergia::importe, VentaEnergia::importe);
    }

    @Test
    public void kwh() {
        testInt64(VentaEnergia::kwh, VentaEnergia::kwh);
    }

    @Test
    public void ocr() {
        testInt64(VentaEnergia::ocr, VentaEnergia::ocr);
    }

    @Test
    public void diasFacturados() {

    }

    @Test
    public void claseVenta() {
    }

    @Test
    public void concepto() {
    }

    @Test
    public void testTotal() {
    }

    @Test
    public void testDap() {
    }

    @Test
    public void testIva() {
    }

    @Test
    public void testImporte() {
    }

    @Test
    public void testKwh() {
    }

    @Test
    public void testOcr() {
    }

    @Test
    public void testDiasFacturados() {
    }

    @Test
    public void testClaseVenta() {
    }

    @Test
    public void testConcepto() {
    }
}
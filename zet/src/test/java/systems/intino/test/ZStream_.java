package systems.intino.test;

import org.junit.Before;
import org.junit.Test;
import systems.intino.datamarts.zet.io.ZInputStream;
import systems.intino.datamarts.zet.io.ZOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.assertj.core.api.Assertions.assertThat;

public class ZStream_ {

	private ByteArrayOutputStream stream;

	private static void eof(ZInputStream input) throws IOException {
		try {
			input.readLong();
			fail("EOF not caught");
		} catch (EOFException e) {
			assertTrue("EOF caught", true);
		}
	}

	@Before
	public void setUp() throws Exception {
		stream = new ByteArrayOutputStream();
	}

	@Test
	public void should_work_with_an_empty_zet() throws IOException {
		ZOutputStream stream = new ZOutputStream(this.stream);
		stream.close();
		byte[] bytes = bytes();
		assertThat(bytes.length).isEqualTo(16);
		assertThat(getLong(bytes, 0)).isEqualTo(0xFFFFFFFFFFFFFFFFL);
		assertThat(getLong(bytes, 8)).isEqualTo(0x0L);

		ZInputStream input = new ZInputStream(new ByteArrayInputStream(bytes));
		eof(input);
	}

	@Test
	public void should_work_with_a_zet_with_one_element() throws IOException {
		ZOutputStream stream = new ZOutputStream(this.stream);
		stream.writeLong(0);
		stream.close();
		byte[] bytes = bytes();
		assertThat(bytes.length).isEqualTo(19);
		assertThat(bytes[0]).isEqualTo((byte) 0);
		assertThat(bytes[1]).isEqualTo((byte) 1);
		assertThat(bytes[2]).isEqualTo((byte) 0);
		assertThat(getLong(bytes, 3)).isEqualTo(0xFFFFFFFFFFFFFFFFL);
		assertThat(getLong(bytes, 11)).isEqualTo(0x1L);

		ZInputStream input = new ZInputStream(new ByteArrayInputStream(bytes));
		assertThat(input.readLong()).isEqualTo(0);
		eof(input);
	}

	@Test
	public void should_work_with_a_zet_with_one_element_of_level_1() throws IOException {
		ZOutputStream stream = new ZOutputStream(this.stream);
		stream.writeLong(0x433);
		stream.close();
		byte[] bytes = bytes();
		assertThat((long) bytes.length).isEqualTo(20);
		assertThat(bytes[0]).isEqualTo((byte) 1);
		assertThat(bytes[1]).isEqualTo((byte) 4);
		assertThat(bytes[2]).isEqualTo((byte) 1);
		assertThat(bytes[3]).isEqualTo((byte) 0x33);
		assertThat(getLong(bytes, 4)).isEqualTo(0xFFFFFFFFFFFFFFFFL);
		assertThat(getLong(bytes, 12)).isEqualTo(0x1L);

		ZInputStream input = new ZInputStream(new ByteArrayInputStream(bytes));
		assertThat(input.readLong()).isEqualTo(0x433);
		eof(input);
	}

	@Test
	public void should_work_with_a_zet_with_one_element_of_level_2() throws IOException {
		ZOutputStream stream = new ZOutputStream(this.stream);
		stream.writeLong(0xAB433);
		stream.close();
		byte[] bytes = bytes();
		assertThat((long) bytes.length).isEqualTo(21);
		assertThat(bytes[0]).isEqualTo((byte) 0x02);
		assertThat(bytes[1]).isEqualTo((byte) 0xA);
		assertThat(bytes[2]).isEqualTo((byte) 0xB4);
		assertThat(bytes[3]).isEqualTo((byte) 0x01);
		assertThat(bytes[4]).isEqualTo((byte) 0x33);
		assertThat(getLong(bytes, 5)).isEqualTo(0xFFFFFFFFFFFFFFFFL);
		assertThat(getLong(bytes, 13)).isEqualTo(0x1L);

		ZInputStream input = new ZInputStream(new ByteArrayInputStream(bytes));
		assertThat(input.readLong()).isEqualTo(0xAB433);
		eof(input);
	}

	@Test
	public void should_work_with_a_zet_with_one_element_of_level_7() throws IOException {
		ZOutputStream stream = new ZOutputStream(this.stream);
		stream.writeLong(0x1122334455667733L);
		stream.close();
		byte[] bytes = bytes();
		assertThat((long) bytes.length).isEqualTo(26);
		assertThat(bytes[0]).isEqualTo((byte) 0x07);
		assertThat(bytes[1]).isEqualTo((byte) 0x11);
		assertThat(bytes[2]).isEqualTo((byte) 0x22);
		assertThat(bytes[3]).isEqualTo((byte) 0x33);
		assertThat(bytes[4]).isEqualTo((byte) 0x44);
		assertThat(bytes[5]).isEqualTo((byte) 0x55);
		assertThat(bytes[6]).isEqualTo((byte) 0x66);
		assertThat(bytes[7]).isEqualTo((byte) 0x77);
		assertThat(bytes[8]).isEqualTo((byte) 0x01);
		assertThat(bytes[9]).isEqualTo((byte) 0x33);
		assertThat(getLong(bytes, 10)).isEqualTo(0xFFFFFFFFFFFFFFFFL);
		assertThat(getLong(bytes, 18)).isEqualTo(0x1L);

		ZInputStream input = new ZInputStream(new ByteArrayInputStream(bytes));
		assertThat(input.readLong()).isEqualTo(0x1122334455667733L);
		eof(input);
	}

	@Test
	public void should_work_with_a_zet_with_256_elements_of_level_6() throws IOException {
		ZOutputStream stream = new ZOutputStream(this.stream);
		long base = 0x11223344556600L;
		for (int i = 0; i < 256; i++) stream.writeLong(base + i);
		stream.close();
		byte[] bytes = bytes();
		assertThat((long) bytes.length).isEqualTo(280);
		assertThat(bytes[0]).isEqualTo((byte) 0x06);
		assertThat(bytes[1]).isEqualTo((byte) 0x11);
		assertThat(bytes[2]).isEqualTo((byte) 0x22);
		assertThat(bytes[3]).isEqualTo((byte) 0x33);
		assertThat(bytes[4]).isEqualTo((byte) 0x44);
		assertThat(bytes[5]).isEqualTo((byte) 0x55);
		assertThat(bytes[6]).isEqualTo((byte) 0x66);
		assertThat(bytes[7]).isEqualTo((byte) 0x00);
		for (int i = 0; i < 256; i++) assertThat(bytes[8 + i]).isEqualTo((byte) i);
		assertThat(getLong(bytes, 264)).isEqualTo(0xFFFFFFFFFFFFFFFFL);
		assertThat(getLong(bytes, 272)).isEqualTo(256L);

		ZInputStream input = new ZInputStream(new ByteArrayInputStream(bytes));
		for (int i = 0; i < 256; i++) assertThat(input.readLong()).isEqualTo(base + i);
		eof(input);
	}

	@Test
	public void should_work_with_a_zet_with_128_elements_of_level_4() throws IOException {
		ZOutputStream stream = new ZOutputStream(this.stream);
		long base = 0x1122334400L;
		for (int i = 0; i < 128; i++) stream.writeLong(base + i);
		stream.close();
		byte[] bytes = bytes();
		assertThat((long) bytes.length).isEqualTo(150);
		assertThat(bytes[0]).isEqualTo((byte) 0x4);
		assertThat(bytes[1]).isEqualTo((byte) 0x11);
		assertThat(bytes[2]).isEqualTo((byte) 0x22);
		assertThat(bytes[3]).isEqualTo((byte) 0x33);
		assertThat(bytes[4]).isEqualTo((byte) 0x44);
		assertThat(bytes[5]).isEqualTo((byte) 0x80);
		for (int i = 0; i < 128; i++) assertThat(bytes[6 + i]).isEqualTo((byte) i);
		assertThat(getLong(bytes, 134)).isEqualTo(0xFFFFFFFFFFFFFFFFL);
		assertThat(getLong(bytes, 142)).isEqualTo(128L);

		ZInputStream input = new ZInputStream(new ByteArrayInputStream(bytes));
		for (int i = 0; i < 128; i++) assertThat(input.readLong()).isEqualTo(base + i);
		eof(input);
	}

	@Test
	public void should_work_with_a_zet_2_sibling_bases_of_128_elements() throws IOException {
		ZOutputStream stream = new ZOutputStream(this.stream);
		long base1 = 0x1122334400L;
		for (int i = 0; i < 128; i++) stream.writeLong(base1 + i);
		long base2 = 0x1122445500L;
		for (int i = 0; i < 128; i++) stream.writeLong(base2 + i);
		stream.close();
		byte[] bytes = bytes();
		assertThat(bytes.length).isEqualTo(282);
		assertThat(bytes[0]).isEqualTo((byte) 0x04);
		assertThat(bytes[1]).isEqualTo((byte) 0x11);
		assertThat(bytes[2]).isEqualTo((byte) 0x22);
		assertThat(bytes[3]).isEqualTo((byte) 0x33);
		assertThat(bytes[4]).isEqualTo((byte) 0x44);
		assertThat(bytes[5]).isEqualTo((byte) 0x80);
		for (int i = 0; i < 128; i++) assertThat(bytes[6 + i]).isEqualTo((byte) i);
		assertThat(bytes[134]).isEqualTo((byte) 0x02);
		assertThat(bytes[135]).isEqualTo((byte) 0x44);
		assertThat(bytes[136]).isEqualTo((byte) 0x55);
		assertThat(bytes[137]).isEqualTo((byte) 0x80);
		for (int i = 0; i < 128; i++) assertThat(bytes[138 + i]).isEqualTo((byte) i);
		assertThat(getLong(bytes, 266)).isEqualTo(0xFFFFFFFFFFFFFFFFL);
		assertThat(getLong(bytes, 274)).isEqualTo(256L);


		ZInputStream input = new ZInputStream(new ByteArrayInputStream(bytes));
		for (int i = 0; i < 128; i++) assertThat(input.readLong()).isEqualTo(base1 + i);
		for (int i = 0; i < 128; i++) assertThat(input.readLong()).isEqualTo(base2 + i);
		eof(input);
	}

	@Test
	public void should_work_with_a_zet_2_bases_of_128_elements() throws IOException {
		ZOutputStream stream = new ZOutputStream(this.stream);
		long base1 = 0x1122334400L;
		for (int i = 0; i < 128; i++) stream.writeLong(base1 + i);
		long base2 = 0xAABBCCDDEE00L;
		for (int i = 0; i < 128; i++) stream.writeLong(base2 + i);
		stream.close();
		byte[] bytes = bytes();
		assertThat((long) bytes.length).isEqualTo(285);
		assertThat(bytes[0]).isEqualTo((byte) 0x04);
		assertThat(bytes[1]).isEqualTo((byte) 0x11);
		assertThat(bytes[2]).isEqualTo((byte) 0x22);
		assertThat(bytes[3]).isEqualTo((byte) 0x33);
		assertThat(bytes[4]).isEqualTo((byte) 0x44);
		assertThat(bytes[5]).isEqualTo((byte) 0x80);
		for (int i = 0; i < 128; i++) assertThat(bytes[6 + i]).isEqualTo((byte) i);
		assertThat(bytes[134]).isEqualTo((byte) 0x05);
		assertThat(bytes[135]).isEqualTo((byte) 0xAA);
		assertThat(bytes[136]).isEqualTo((byte) 0xBB);
		assertThat(bytes[137]).isEqualTo((byte) 0xCC);
		assertThat(bytes[138]).isEqualTo((byte) 0xDD);
		assertThat(bytes[139]).isEqualTo((byte) 0xEE);
		assertThat(bytes[140]).isEqualTo((byte) 0x80);
		for (int i = 0; i < 128; i++) assertThat(bytes[141 + i]).isEqualTo((byte) i);
		assertThat(getLong(bytes, 269)).isEqualTo(0xFFFFFFFFFFFFFFFFL);
		assertThat(getLong(bytes, 277)).isEqualTo(256L);


		ZInputStream input = new ZInputStream(new ByteArrayInputStream(bytes));
		for (int i = 0; i < 128; i++) assertThat(input.readLong()).isEqualTo(base1 + i);
		for (int i = 0; i < 128; i++) assertThat(input.readLong()).isEqualTo(base2 + i);
		eof(input);
	}

	private long getLong(byte[] bytes, int pos) {
		byte[] by = new byte[8];
		System.arraycopy(bytes, pos, by, 0, 8);
		long value = 0;
		for (byte b : by) value = (value << 8) + ((long) b & 0xFF);
		return value;
	}

	private byte[] bytes() {
		return stream.toByteArray();
	}
}

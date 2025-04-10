package tests;

import org.junit.Test;
import systems.intino.alexandria.datamarts.subjectstore.SubjectStore;
import systems.intino.alexandria.datamarts.subjectstore.io.registries.SqliteConnection;
import systems.intino.alexandria.datamarts.subjectstore.model.Point;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;

import static java.time.temporal.ChronoUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectStore_ {
	private static final Instant now = Instant.now().truncatedTo(DAYS);
	private static final Instant day = Instant.parse("2025-03-25T00:00:00Z");
	private static final String categories = "DEPOLARISE";

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Test
	public void should_handle_empty_store() {
		File file = new File("patient.oss");
		try (SubjectStore store = new SubjectStore("123:patient", file)) {
			assertThat(store.id()).isEqualTo("123");
			assertThat(store.type()).isEqualTo("patient");
			assertThat(store.name()).isEqualTo("123:patient");
			assertThat(store.size()).isEqualTo(0);
			assertThat(store.exists("field")).isFalse();
			assertThat(store.currentNumber("field")).isNull();
			assertThat(store.currentText("field")).isNull();
			assertThat(store.categoricalQuery("field").get()).isNull();
			assertThat(store.legacyExists()).isFalse();
			assertThat(store.bigbangExists()).isFalse();
			assertThat(store.instants()).isEmpty();
		}
		finally {
			file.delete();
		}
	}

	@Test
	public void should_ignore_feed_without_data() throws IOException {
		File file = File.createTempFile("port", ".oss");
		try (SubjectStore store = new SubjectStore("00000", file)) {
			store.feed(Instant.now(), "Skip").terminate();
			assertThat(store.size()).isEqualTo(0);
		}
	}

	@Test
	public void should_return_most_recent_value_as_current() throws IOException {
		File file = File.createTempFile("patient", ".oss");
		try (SubjectStore store = new SubjectStore("12345:patient", file)) {
			feed_batch(store);
			test_batch(store);
		}
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Test
	public void should_dump_and_restore_events() throws IOException {
		File file = new File("patient.oss");
		OutputStream os = new ByteArrayOutputStream();
		try (SubjectStore store = new SubjectStore("12345:patient", file)) {
			feed_batch(store);
			store.dump(os);
		}
		finally {
			file.delete();
		}
		String dump = os.toString();
		test_dump(dump);
		InputStream is = new ByteArrayInputStream(dump.getBytes());
		try (SubjectStore store = new SubjectStore("12345:patient", file)) {
			store.restore(is);
			test_batch(store);
		}
		finally {
			file.delete();
		}
	}


	@Test
	public void should_store_legacy_values() throws IOException {
		File file = File.createTempFile("port", ".oss");
		try (SubjectStore store = new SubjectStore("00000", file)) {
			store.feed(Legacy, "UN:all-ports")
					.add("Country", "China")
					.add("Latitude", 31_219832454L)
					.add("Longitude", 121_486998052L)
					.terminate();
			test_stored_legacy_values(store);
		}
		try (SubjectStore store = new SubjectStore("00000", file)) {
			test_stored_legacy_values(store);
		}
	}

	private static void test_stored_legacy_values(SubjectStore store) {
		assertThat(store.size()).isEqualTo(1);
		assertThat(store.first()).isEqualTo(Legacy);
		assertThat(store.last()).isEqualTo(Legacy);
		assertThat(store.tags()).containsExactly("Country", "Latitude", "Longitude");
		assertThat(store.ss(0)).isEqualTo("UN:all-ports");
		assertThat(store.exists("Country")).isTrue();
		assertThat(store.exists("Latitude")).isTrue();
		assertThat(store.exists("Longitude")).isTrue();
		assertThat(store.categoricalQuery("Country").get()).isNull();
		assertThat(store.categoricalQuery("Latitude").get()).isEqualTo(null);
		assertThat(store.categoricalQuery("Longitude").get()).isEqualTo(null);

		assertThat(store.categoricalQuery("Country").get(LegacyPhase).values()).containsExactly("China");
		assertThat(store.categoricalQuery("Country").get(BigBangPhase).values()).containsExactly();
		assertThat(store.categoricalQuery("Country").get(LastYearWindow).values()).containsExactly();
		assertThat(store.categoricalQuery("Country").get(LastMonthWindow).values()).containsExactly();
		assertThat(store.categoricalQuery("Country").get(LastDayWindow).values()).containsExactly();
		assertThat(store.categoricalQuery("Country").get(LastHourWindow).values()).containsExactly();
		assertThat(store.numericalQuery("Latitude").get(store.first(), store.last()).values()).containsExactly(31_219832454L);
		assertThat(store.numericalQuery("Latitude").get(LegacyPhase).values()).containsExactly(31_219832454L);
		assertThat(store.numericalQuery("Latitude").get(BigBangPhase).values()).containsExactly();
		assertThat(store.numericalQuery("Latitude").get(ThisYear).values()).containsExactly();
		assertThat(store.numericalQuery("Latitude").get(ThisMonth).values()).containsExactly();
		assertThat(store.numericalQuery("Latitude").get(Today).values()).containsExactly();
		assertThat(store.numericalQuery("Latitude").get(ThisHour).values()).containsExactly();
		assertThat(store.numericalQuery("Longitude").get(LegacyPhase).values()).containsExactly(121_486998052L);
		assertThat(store.numericalQuery("Longitude").get(BigBangPhase).values()).containsExactly();

		assertThat(store.legacyExists()).isTrue();
		assertThat(store.bigbangExists()).isFalse();
		assertThat(store.legacyPending()).isTrue();
	}

	@Test
	public void should_store_features() throws IOException {
		File file = File.createTempFile("port", ".oss");
		try (SubjectStore store = new SubjectStore("00000", file)) {
			store.feed(now, "UN:all-ports")
					.add("Country", "China")
					.add("Latitude", 31.219832454)
					.add("Longitude", 121.486998052)
					.terminate();
			test_stored_features(store);
		}
		try (SubjectStore store = new SubjectStore("00000", file)) {
			test_stored_features(store);
		}
	}

	private static void test_stored_features(SubjectStore store) {
		assertThat(store.id()).isEqualTo("00000");
		assertThat(store.size()).isEqualTo(1);
		assertThat(store.first()).isEqualTo(now);
		assertThat(store.last()).isEqualTo(now);
		assertThat(store.tags()).containsExactly("Country", "Latitude", "Longitude");
		assertThat(store.ss(0)).isEqualTo("UN:all-ports");
		assertThat(store.currentNumber("Latitude")).isEqualTo(31.219832454);
		assertThat(store.numericalQuery("Latitude").get()).isEqualTo(value(0, now, 31.219832454));
		assertThat(store.numericalQuery("Longitude").get()).isEqualTo(value(0, now, 121.486998052));
		assertThat(store.numericalQuery("Longitude").get(today(), today(1)).values()).containsExactly(121.486998052);
		assertThat(store.categoricalQuery("Country").get()).isEqualTo(value(0, now, "China"));
		assertThat(store.categoricalQuery("Country").get()).isEqualTo(value(0, now, "China"));
		assertThat(store.categoricalQuery("Country").get(today(), today(1)).count()).isEqualTo(1);
		assertThat(store.categoricalQuery("Country").get(today(), today(1)).values()).containsExactly("China");
		assertThat(store.instants()).containsExactly(now);
	}

	@Test
	public void should_store_time_series() throws IOException {
		File file = File.createTempFile("port", ".oss");
		try (SubjectStore store = new SubjectStore("00000", file)) {
			feed_time_series(store);
			test_stored_time_series(store);
		}
		try (SubjectStore store = new SubjectStore("00000", file)) {
			test_stored_time_series(store);
		}
	}

	@Test
	public void should_create_memory_databases() {
		try (SubjectStore store = new SubjectStore("00000")) {
			feed_time_series(store);
			test_stored_time_series(store);
		}
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Test
	public void should_include_several_subjects() throws SQLException {
		File file = new File("subjects.oss");
		try (Connection connection = SqliteConnection.from(file)) {
			SubjectStore[] stores = new SubjectStore[]{
					new SubjectStore("00001", connection),
					new SubjectStore("00002", connection),
					new SubjectStore("00003", connection),
					new SubjectStore("00004", connection)
			};
			for (SubjectStore store : stores) {
				feed_time_series(store);
				test_stored_time_series(store);
			}
		} finally {
			file.delete();
		}
	}

	@Test
	public void name() {

	}

	private static void feed_time_series(SubjectStore store) {
		for (int i = 0; i < 10; i++) {
			store.feed(today(i), "AIS:movements-" + i)
					.add("Vessels", 1900 + i * 10)
					.add("State", categories.substring(i, i + 1))
					.terminate();
		}
	}

	private void test_stored_time_series(SubjectStore store) {
		assertThat(store.size()).isEqualTo(10);
		assertThat(store.first()).isEqualTo(today());
		assertThat(store.last()).isEqualTo(today(9));
		assertThat(store.tags()).containsExactly("Vessels", "State");
		assertThat(store.ss(0)).isEqualTo("AIS:movements-0");
		assertThat(store.ss(9)).isEqualTo("AIS:movements-9");
		assertThat(store.numericalQuery("Vessels").get()).isEqualTo(value(9, today(9), 1990.0));
		assertThat(store.numericalQuery("Vessels").get(today(200), today(300)).isEmpty()).isTrue();
		assertThat(store.numericalQuery("Vessels").get(today(-200), today(-100)).isEmpty()).isTrue();
		assertThat(store.numericalQuery("Vessels").getAll().values()).containsExactly(1900L, 1910L, 1920L, 1930L, 1940L, 1950L, 1960L, 1970L, 1980L, 1990L);
		assertThat(store.categoricalQuery("State").getAll().values()).containsExactly("D", "E", "P", "O", "L", "A", "R", "I", "S", "E");
		assertThat(store.categoricalQuery("State").getAll().distinct()).containsExactly("D", "E", "P", "O", "L", "A", "R", "I", "S");
		assertThat(store.categoricalQuery("State").get()).isEqualTo(value(9, today(9), "E"));
		assertThat(store.categoricalQuery("State").get(today(0), today(10)).summary().mode()).isEqualTo("E");
	}

	private static void feed_batch(SubjectStore store) {
		SubjectStore.Batch batch = store.batch();
		batch.feed(day, "HMG-2")
				.add("hemoglobin", 145)
				.terminate();

		batch.feed(day.plus(-5, DAYS), "HMG-1")
				.add("hemoglobin", 130)
				.terminate();

		batch.feed(day.plus(-3, DAYS), "HMG-B")
				.add("hemoglobin", 115)
				.terminate();

		batch.feed(day.plus(-20, DAYS), "HMG-L")
				.add("hemoglobin", 110)
				.terminate();

		batch.terminate();
	}

	private static void test_dump(String dump) {
		assertThat(dump).isEqualTo("""
				[patient]
				ts=2025-03-25T00:00:00Z
				ss=HMG-2
				id=12345
				hemoglobin=145.0
				[patient]
				ts=2025-03-20T00:00:00Z
				ss=HMG-1
				id=12345
				hemoglobin=130.0
				[patient]
				ts=2025-03-22T00:00:00Z
				ss=HMG-B
				id=12345
				hemoglobin=115.0
				[patient]
				ts=2025-03-05T00:00:00Z
				ss=HMG-L
				id=12345
				hemoglobin=110.0
				"""
		);
	}

	@SuppressWarnings("SameParameterValue")
	private static <T> Point<T> value(int feed, Instant instant, T value) {
		return new Point<>(feed, instant, value);
	}

	private static void test_batch(SubjectStore store) {
		assertThat(store.type()).startsWith("patient");
		assertThat(store.id()).isEqualTo("12345");
		assertThat(store.currentNumber("hemoglobin")).isEqualTo(145.0);
		Point<Double> actual = store.numericalQuery("hemoglobin").get();
		assertThat(actual.value()).isEqualTo(145);
		assertThat(store.instants()).containsExactly(day.plus(-20, DAYS), day.plus(-5, DAYS), day.plus(-3, DAYS), day);
	}
}

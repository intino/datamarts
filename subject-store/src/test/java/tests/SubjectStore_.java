package tests;

import systems.intino.alexandria.datamarts.model.Point;
import systems.intino.alexandria.datamarts.SubjectStore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;

import static java.time.temporal.ChronoUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;
import static systems.intino.alexandria.datamarts.model.TemporalReferences.*;
import static systems.intino.alexandria.datamarts.model.TemporalReferences.TimeSpan.*;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectStore_ {
	private static final Instant now = Instant.now().truncatedTo(DAYS);
	private static final Instant day = Instant.parse("2025-03-25T00:00:00Z");
	private static final String categories = "DEPOLARISE";

	@Test
	public void should_create_memory_databases() throws IOException {
		try (SubjectStore store = new SubjectStore("00000")) {
			assertThat(store.size()).isEqualTo(0);
			assertThat(store.name()).isEqualTo("00000:subject");
			assertThat(store.id()).isEqualTo("00000");
			assertThat(store.type()).isEqualTo("subject");
			store.feed(Instant.now(), "")
					.add("weight", 82.5)
					.terminate();
			assertThat(store.size()).isEqualTo(1);
			assertThat(store.numericalQuery("weight").current().value()).isEqualTo(82.5);
		}
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Test
	public void should_handle_empty_store() throws IOException {
		File file = new File("patient.oss");
		try (SubjectStore store = new SubjectStore(file, "123")) {
			assertThat(store.id()).isEqualTo("123");
			assertThat(store.type()).isEqualTo("patient");
			assertThat(store.name()).isEqualTo("123:patient");
			assertThat(store.size()).isEqualTo(0);
			assertThat(store.exists("field")).isFalse();
			assertThat(store.currentNumber("field")).isNull();
			assertThat(store.currentText("field")).isNull();
			//assertThat(store.numericalQuery("field").signal(store.first(), store.last())).isEmpty();
			assertThat(store.categoricalQuery("field").current()).isNull();
			//assertThat(store.categoricalQuery("field").sequence(store.first(), store.last())).isEmpty();
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
		try (SubjectStore store = new SubjectStore(file, "00000")) {
			store.feed(Instant.now(), "Skip").terminate();
			assertThat(store.size()).isEqualTo(0);
		}
	}

	@Test
	public void should_return_most_recent_value_as_current() throws IOException {
		File file = File.createTempFile("patient", ".oss");
		try (SubjectStore store = new SubjectStore(file, "12345")) {
			feed(store);
			assertThat(store.type()).startsWith("patient");
			assertThat(store.id()).isEqualTo("12345");
			assertThat(store.currentNumber("hemoglobin")).isEqualTo(145.0);
			Point<Double> actual = store.numericalQuery("hemoglobin").current();
			assertThat(actual.value()).isEqualTo(145);
			assertThat(store.legacyExists()).isTrue();
			assertThat(store.bigbangExists()).isTrue();
			assertThat(store.legacyPending()).isFalse();
			assertThat(store.instants()).containsExactly(Legacy, BigBang, day.plus(-5, DAYS), day);
		}
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Test
	public void should_dump_events() throws IOException {
		File file = new File("patient.oss");
		try (SubjectStore store = new SubjectStore(file, "12345")) {
			feed(store);
			OutputStream os = new ByteArrayOutputStream();
			store.dump(os);
			assertThat(os.toString()).isEqualTo("""
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
				ts=-314918-08-13T06:13:20Z
				ss=HMG-B
				id=12345
				hemoglobin=115.0
				[patient]
				ts=-1899355-09-09T13:20:00Z
				ss=HMG-L
				id=12345
				hemoglobin=110.0
				"""
			);
		}
		finally {
			file.delete();
		}
	}

	private static void feed(SubjectStore store) {
		SubjectStore.Batch batch = store.batch();
		batch.feed(day, "HMG-2")
				.add("hemoglobin", 145)
				.terminate();

		batch.feed(day.plus(-5, DAYS), "HMG-1")
				.add("hemoglobin", 130)
				.terminate();

		batch.feed(BigBang, "HMG-B")
				.add("hemoglobin", 115)
				.terminate();

		batch.feed(Legacy, "HMG-L")
				.add("hemoglobin", 110)
				.terminate();

		batch.terminate();
	}


	@Test
	public void should_store_legacy_values() throws IOException {
		File file = File.createTempFile("port", ".oss");
		try (SubjectStore store = new SubjectStore(file, "00000")) {
			store.feed(Legacy, "UN:all-ports")
					.add("Country", "China")
					.add("Latitude", 31_219832454L)
					.add("Longitude", 121_486998052L)
					.terminate();
			test_stored_legacy_values(store);
		}
		try (SubjectStore store = new SubjectStore(file, "00000")) {
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
		assertThat(store.categoricalQuery("Country").current()).isNull();
		assertThat(store.categoricalQuery("Latitude").current()).isEqualTo(null);
		assertThat(store.categoricalQuery("Longitude").current()).isEqualTo(null);

		assertThat(store.categoricalQuery("Country").sequence(LegacyPhase).values()).containsExactly("China");
		assertThat(store.categoricalQuery("Country").sequence(BigBangPhase).values()).containsExactly();
		assertThat(store.categoricalQuery("Country").sequence(LastYearWindow).values()).containsExactly();
		assertThat(store.categoricalQuery("Country").sequence(LastMonthWindow).values()).containsExactly();
		assertThat(store.categoricalQuery("Country").sequence(LastDayWindow).values()).containsExactly();
		assertThat(store.categoricalQuery("Country").sequence(LastHourWindow).values()).containsExactly();
		assertThat(store.numericalQuery("Latitude").signal(store.first(), store.last()).values()).containsExactly(31_219832454L);
		assertThat(store.numericalQuery("Latitude").signal(LegacyPhase).values()).containsExactly(31_219832454L);
		assertThat(store.numericalQuery("Latitude").signal(BigBangPhase).values()).containsExactly();
		assertThat(store.numericalQuery("Latitude").signal(ThisYear).values()).containsExactly();
		assertThat(store.numericalQuery("Latitude").signal(ThisMonth).values()).containsExactly();
		assertThat(store.numericalQuery("Latitude").signal(Today).values()).containsExactly();
		assertThat(store.numericalQuery("Latitude").signal(ThisHour).values()).containsExactly();
		assertThat(store.numericalQuery("Longitude").signal(LegacyPhase).values()).containsExactly(121_486998052L);
		assertThat(store.numericalQuery("Longitude").signal(BigBangPhase).values()).containsExactly();

		assertThat(store.legacyExists()).isTrue();
		assertThat(store.bigbangExists()).isFalse();
		assertThat(store.legacyPending()).isTrue();
	}

	@Test
	public void should_store_features() throws IOException {
		File file = File.createTempFile("port", ".oss");
		try (SubjectStore store = new SubjectStore(file, "00000")) {
			store.feed(now, "UN:all-ports")
					.add("Country", "China")
					.add("Latitude", 31.219832454)
					.add("Longitude", 121.486998052)
					.terminate();
			test_stored_features(store);
		}
		try (SubjectStore store = new SubjectStore(file,"00000")) {
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
		assertThat(store.numericalQuery("Latitude").current()).isEqualTo(value(0, now, 31.219832454));
		assertThat(store.numericalQuery("Longitude").current()).isEqualTo(value(0, now, 121.486998052));
		assertThat(store.numericalQuery("Longitude").signal(today(), today(1)).values()).containsExactly(121.486998052);
		assertThat(store.categoricalQuery("Country").current()).isEqualTo(value(0, now, "China"));
		assertThat(store.categoricalQuery("Country").current()).isEqualTo(value(0, now, "China"));
		assertThat(store.categoricalQuery("Country").sequence(today(), today(1)).count()).isEqualTo(1);
		assertThat(store.categoricalQuery("Country").sequence(today(), today(1)).values()).containsExactly("China");
		assertThat(store.instants()).containsExactly(now);
	}

	@SuppressWarnings("SameParameterValue")
	private static <T> Point<T> value(int feed, Instant instant, T value) {
		return new Point<>(feed, instant, value);
	}

	@Test
	public void should_store_time_series() throws IOException {
		File file = File.createTempFile("port", ".oss");
		try (SubjectStore store = new SubjectStore(file, "00000")) {
			for (int i = 0; i < 10; i++) {
				store.feed(today(i), "AIS:movements-" + i)
						.add("Vessels", 1900 + i * 10)
						.add("State", categories.substring(i, i + 1))
						.terminate();
			}
			test_stored_time_series(store);
		}
		try (SubjectStore store = new SubjectStore(file, "00000")) {
			test_stored_time_series(store);
		}
	}

	private void test_stored_time_series(SubjectStore store) {
		assertThat(store.size()).isEqualTo(10);
		assertThat(store.first()).isEqualTo(today());
		assertThat(store.last()).isEqualTo(today(9));
		assertThat(store.tags()).containsExactly("Vessels", "State");
		assertThat(store.ss(0)).isEqualTo("AIS:movements-0");
		assertThat(store.ss(9)).isEqualTo("AIS:movements-9");
		assertThat(store.numericalQuery("Vessels").current()).isEqualTo(value(9, today(9), 1990.0));
		assertThat(store.numericalQuery("Vessels").signal(today(200), today(300)).isEmpty()).isTrue();
		assertThat(store.numericalQuery("Vessels").signal(today(-200), today(-100)).isEmpty()).isTrue();
		assertThat(store.numericalQuery("Vessels").signal(store.first(), store.last()).values()).containsExactly(1900L, 1910L, 1920L, 1930L, 1940L, 1950L, 1960L, 1970L, 1980L, 1990L);
		assertThat(store.categoricalQuery("State").current()).isEqualTo(value(9, today(9), "E"));
		assertThat(store.categoricalQuery("State").sequence(today(0), today(10)).summary().mode()).isEqualTo("E");
	}


}

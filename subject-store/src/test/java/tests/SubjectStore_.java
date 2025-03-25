package tests;

import io.intino.alexandria.datamarts.model.Point;
import io.intino.alexandria.datamarts.SubjectStore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static io.intino.alexandria.datamarts.model.TemporalReferences.*;
import static io.intino.alexandria.datamarts.model.TemporalReferences.TimeSpan.*;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectStore_ {
	private static final Instant now = Instant.now().truncatedTo(ChronoUnit.DAYS);
	private static final String categories = "DEPOLARISE";

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Test
	public void should_handle_empty_store() throws IOException {
		File file = new File("sub:123.oss");
		try (SubjectStore store = new SubjectStore(file)) {
			assertThat(store.name()).isEqualTo("sub:123");
			assertThat(store.toString()).isEqualTo("sub:123");
			assertThat(store.feeds()).isEqualTo(0);
			assertThat(store.exists("field")).isFalse();
			assertThat(store.numericalQuery("field").current()).isNull();
			assertThat(store.numericalQuery("field").signal(store.first(), store.last())).isEmpty();
			assertThat(store.categoricalQuery("field").current()).isNull();
			assertThat(store.categoricalQuery("field").sequence(store.first(), store.last())).isEmpty();
			assertThat(store.legacyExists()).isFalse();
			assertThat(store.bigbangExists()).isFalse();
			assertThat(store.instants()).isEmpty();
		}
		finally {
			file.delete();
		}
	}

	@Test
	public void should_return_most_recent_value_as_current() throws IOException {
		File file = File.createTempFile("patient", ".oss");

		try (SubjectStore store = new SubjectStore(file)) {
			store.feed(today(), "HMG-2")
					.add("Hemoglobin", 145)
					.execute();

			store.feed(today(-5), "HMG-1")
					.add("Hemoglobin", 130)
					.execute();

			store.feed(BigBang, "HMG-B")
					.add("Hemoglobin", 115)
					.execute();

			store.feed(Legacy, "HMG-L")
					.add("Hemoglobin", 110)
					.execute();

			Point<Double> actual = store.numericalQuery("Hemoglobin").current();
			assertThat(actual.value()).isEqualTo(145L);
			assertThat(actual.instant()).isEqualTo(today());
			assertThat(store.legacyExists()).isTrue();
			assertThat(store.bigbangExists()).isTrue();
			assertThat(store.legacyPending()).isFalse();
			assertThat(store.instants()).containsExactly(Legacy, BigBang, today(-5), today());
		}
	}

	@Test
	public void should_store_legacy_values() throws IOException {
		File file = File.createTempFile("port", ".oss");
		try (SubjectStore store = new SubjectStore(file)) {
			store.feed(Legacy, "UN:all-ports")
					.add("Country", "China")
					.add("Latitude", 31_219832454L)
					.add("Longitude", 121_486998052L)
					.execute();
			test_stored_legacy_values(store);
		}
		try (SubjectStore store = new SubjectStore(file)) {
			test_stored_legacy_values(store);
		}
	}

	private static void test_stored_legacy_values(SubjectStore store) {
		assertThat(store.feeds()).isEqualTo(1);
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
		try (SubjectStore store = new SubjectStore(file)) {
			store.feed(now, "UN:all-ports")
					.add("Country", "China")
					.add("Latitude", 31.219832454)
					.add("Longitude", 121.486998052)
					.execute();
			test_stored_features(store);
		}
		try (SubjectStore store = new SubjectStore(file)) {
			test_stored_features(store);
		}
	}

	private static void test_stored_features(SubjectStore store) {
		assertThat(store.feeds()).isEqualTo(1);
		assertThat(store.first()).isEqualTo(now);
		assertThat(store.last()).isEqualTo(now);
		assertThat(store.tags()).containsExactly("Country", "Latitude", "Longitude");
		assertThat(store.ss(0)).isEqualTo("UN:all-ports");
		assertThat(store.categoricalQuery("Country").current()).isEqualTo(value(0, now, "China"));
		assertThat(store.categoricalQuery("Country").current()).isEqualTo(value(0, now, "China"));
		assertThat(store.categoricalQuery("Country").sequence(today(0), today(1)).count()).isEqualTo(1);
		assertThat(store.categoricalQuery("Country").sequence(today(0), today(1)).summary().categories()).containsExactly("China");
		assertThat(store.numericalQuery("Latitude").current()).isEqualTo(value(0, now, 31.219832454));
		assertThat(store.numericalQuery("Longitude").current()).isEqualTo(value(0, now, 121.486998052));
		assertThat(store.instants()).containsExactly(now);
	}

	@SuppressWarnings("SameParameterValue")
	private static <T> Point<T> value(int feed, Instant instant, T value) {
		return new Point<>(feed, instant, value);
	}

	@Test
	public void should_store_time_series() throws IOException {
		File file = File.createTempFile("port", ".oss");
		try (SubjectStore store = new SubjectStore(file)) {
			for (int i = 0; i < 10; i++) {
				store.feed(today(i), "AIS:movements-" + i)
						.add("Vessels", 1900 + i * 10)
						.add("State", categories.substring(i, i + 1))
						.execute();
			}
			test_stored_time_series(store);
		}
		try (SubjectStore store = new SubjectStore(file)) {
			test_stored_time_series(store);
		}
	}

	private void test_stored_time_series(SubjectStore store) {
		assertThat(store.feeds()).isEqualTo(10);
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

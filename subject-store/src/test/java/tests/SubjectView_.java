package tests;

import systems.intino.alexandria.datamarts.SubjectView;
import systems.intino.alexandria.datamarts.SubjectStore;
import systems.intino.alexandria.datamarts.model.filters.MinMaxNormalizationFilter;
import systems.intino.alexandria.datamarts.model.filters.RollingAverageFilter;
import systems.intino.alexandria.datamarts.model.view.Column;
import systems.intino.alexandria.datamarts.model.view.Format;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import static java.time.temporal.ChronoUnit.*;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectView_ {
	private final static Instant from = Instant.parse("2025-01-01T00:00:00Z");
	private final static Instant to = Instant.parse("2025-02-01T00:00:00Z");
	private final static String expected = """
		2025	1	1	0.0	0.0	0.0				0.0	0.0
		2025	1	8	48.0	24.0	1.0		28.0	cloudy	1.0	100.0
		2025	1	15	0.0	0.0	0.0	8.0			0.0	0.0
		2025	1	22	0.0	0.0	0.0	8.0			0.0	0.0
		2025	1	29	18.0	18.0	0.375	6.0	18.0	rain	1.0	37.5
		""";

	@Test
	public void should_export_To_tabular_report_with_temporal_numerical_and_categorical_columns() throws IOException {
		try (SubjectStore store = new SubjectStore("map", File.createTempFile("xyz", ":patient.oss"))) {
			feed(store);
			Format format = new Format(from, to, Duration.ofDays(7));
			format.add(new Column("Year=year"));
			format.add(new Column("Month=month-of-year"));
			format.add(new Column("Day=day-of-month"));

			format.add(new Column("TotalTemp=temperature.sum"));
			format.add(new Column("AvgTemp=temperature.average"));
			format.add(new Column("NormTemp=TotalTemp").add(new MinMaxNormalizationFilter()));
			format.add(new Column("Trend=AvgTemp").add(new RollingAverageFilter(3)));

			format.add(new Column("LastTemp=temperature.last"));
			format.add(new Column("SkyMode=sky.mode"));
			format.add(new Column("SkyCount=sky.count"));
			format.add(new Column("NewTemp=NormTemp * 100"));
			SubjectView table = new SubjectView(store, format);
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			table.exportTo(os);
			assertThat(os.toString()).isEqualTo(expected);
		}
	}

	private void feed(SubjectStore store) {
		store.feed(from.plus(10, DAYS), "test")
				.add("temperature", 20)
				.terminate();
		store.feed(from.plus(12, DAYS), "test")
				.add("temperature", 28)
				.add("sky", "cloudy")
				.terminate();
		store.feed(from.plus(28, DAYS), "test")
				.add("temperature", 18)
				.add("sky", "rain")
				.terminate();
	}
}

package tests;

import systems.intino.alexandria.datamarts.SubjectView;
import systems.intino.alexandria.datamarts.SubjectStore;
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
import static systems.intino.alexandria.datamarts.model.view.functions.CategoricalFunction.Mode;
import static systems.intino.alexandria.datamarts.model.view.functions.NumericalFunction.*;
import static systems.intino.alexandria.datamarts.model.view.functions.TemporalFunction.*;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectView_ {
	private final static Instant from = Instant.parse("2025-01-01T00:00:00Z");
	private final static Instant to = Instant.parse("2025-02-01T00:00:00Z");
	private final static String expected = """
		1	1	0.0	0.0				20250101
		8	1	1.0	24.0	20.0	28.0		20250108
		15	1	0.0	0.0				20250115
		22	1	0.0	0.0				20250122
		29	1	0.75	18.0	18.0	18.0		20250129
		""";

	@Test
	public void should_export_tabular_report_with_temporal_numerical_and_categorical_columns() throws IOException {
		try (SubjectStore store = new SubjectStore(File.createTempFile("xyz", ":patient.oss"))) {
			feed(store);
			Format format = new Format(from, to, Duration.ofDays(7));
			format.add(new Column.Temporal(DayOfMonth));
			format.add(new Column.Temporal(MonthOfYear));
			format.add(new Column.Numerical("temperature", Average));
			format.add(new Column.Numerical("temperature", Average));
			format.add(new Column.Numerical("temperature", First));
			format.add(new Column.Numerical("temperature", Last));
			format.add(new Column.Categorical("sky", Mode));
			format.add(new Column.Temporal(YearMonthDay));
			SubjectView table = new SubjectView(store, format).normalize(2);
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			table.export(os);
			assertThat(os.toString()).isEqualTo(expected.trim());
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

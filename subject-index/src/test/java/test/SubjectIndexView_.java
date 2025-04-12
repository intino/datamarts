package test;

import org.junit.Test;
import systems.intino.datamarts.subjectindex.SubjectIndex;
import systems.intino.datamarts.subjectindex.SubjectIndexView;
import systems.intino.datamarts.subjectindex.io.StatementFeeder;
import systems.intino.datamarts.subjectindex.model.Subject;
import systems.intino.datamarts.subjectindex.model.Subjects;
import systems.intino.datamarts.subjectindex.view.Column;
import systems.intino.datamarts.subjectindex.view.Summary;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectIndexView_ {

	@Test
	public void should_create_views_including_summary() throws IOException {
		try (SubjectIndex index = new SubjectIndex().restore(inputStream("subjects.txt"))) {
			SubjectIndexView subjects = SubjectIndexView.of(index)
					.select("model")
					.add("status")
					.add("name")
					.build();
			SubjectIndexView experiments = SubjectIndexView.of(index)
					.select("experiment")
					.add("status")
					.build();

			assertThat(subjects.size()).isEqualTo(25);
			assertThat(subjects.column("status").summary().categories()).containsExactly("active");
			assertThat(subjects.column("name").summary().categories().size()).isEqualTo(25);
			assertThat(experiments.size()).isEqualTo(100);
			assertThat(experiments.column("status").summary().categories()).containsExactly("running", "queued", "error", "done");
			assertThat(experiments.column("status").summary().frequency("running")).isEqualTo(25);
		}
	}

	@Test
	public void should_calculate_summary_frequencies_consistently() throws IOException {
		try (SubjectIndex index = new SubjectIndex().consume(feeder())) {
			assertThat(index.subjects().roots().size()).isEqualTo(435);
			SubjectIndexView view = SubjectIndexView.of(index)
					.select("port")
					.add("country")
					.add("cabotage-region")
					.add("draft")
					.add("cost-per-full")
					.add("cost-per-full-transfer")
					.build();
			for (Column column : view) {
				Summary summary = column.summary();
				List<Subject> all = new ArrayList<>(index.subjects("port").all().items());
				for (String category : summary.categories()) {
					if (category.equals("N/A")) continue;
					Subjects subjects = index.subjects("port").with(column.name(), category).all();
					all.removeAll(subjects.items());
					assertThat(summary.frequency(category)).isEqualTo(subjects.size());
				}
				assertThat(summary.frequency("N/A")).isEqualTo(all.size());
			}
		}
	}

	private StatementFeeder feeder() {
		StatementFeeder feeder = StatementFeeder.fromTabularData(inputStream("ports.tsv"));
		feeder.schema()
				.map("id", s-> s + ".port")
				.map("latitude", s-> null)
				.map("longitude", s-> null)
				.map("id", s-> s + ".port")
				.map("draft", this::range2)
				.map("cost-per-full", this::range100)
				.map("cost-per-full-transfer", this::range100)
				.map("cost-port-call-fixed", this::range100)
				.map("cost-port-call-per-ffe", this::range100);
		return feeder;
	}

	private String range2(String value) {
		return range(value, 2);
	}

	private String range100(String value) {
		return range(value, 100);
	}

	private static String range(String value, int bin) {
		int v = (int) (Double.parseDouble(value) / bin);
		return '[' + String.valueOf(v * bin) + "-" + (v + 1) * bin + ')';
	}


	private static InputStream inputStream(String name) {
		return SubjectIndex_.class.getClassLoader().getResourceAsStream(name);
	}
}

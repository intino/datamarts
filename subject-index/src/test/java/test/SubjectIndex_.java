package test;

import org.junit.Test;
import systems.intino.alexandria.datamarts.subjectmap.SubjectIndex;
import systems.intino.alexandria.datamarts.subjectmap.model.Subject;
import systems.intino.alexandria.datamarts.subjectmap.model.Subjects;
import systems.intino.alexandria.datamarts.subjectmap.model.Token;

import java.io.*;

import static org.assertj.core.api.Assertions.assertThat;
import static systems.intino.alexandria.datamarts.subjectmap.model.Subject.Any;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectIndex_ {

	@Test
	public void should_support_index_subject_and_conditional_query() throws IOException {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.on("11.o").set("name", "jose").commit();
			check(index);
		}
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.on("11.o").set("name", "jose").commit();
			check(index);
		}
	}

	private static void check(SubjectIndex index) {
		assertThat(index.subjects(Any).roots().serialize()).isEqualTo("11.o");
		assertThat(index.subjects(Any).with("name", "jose").roots().serialize()).isEqualTo("11.o");
		assertThat(index.subjects(Any).without("name", "jose").roots().serialize()).isEqualTo("");
		assertThat(index.subjects(Any).without("name", "mario").roots().serialize()).isEqualTo("11.o");
		assertThat(index.subjects(Any).without("name", "mario").roots().filter(a -> a.is("o")).serialize()).isEqualTo("11.o");
		assertThat(index.subjects(Any).without("name", "mario").roots().filter(a -> a.is("user")).serialize()).isEqualTo("");
	}

	@Test
	public void should_support_rename_subjects() throws IOException {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.on("11.o").set("name", "jose").commit();
			index.on("11.o").rename("22.o").commit();
			checkRename(index);
		}
		try (SubjectIndex index = new SubjectIndex(file)) {
			checkRename(index);
		}
	}

	private static void checkRename(SubjectIndex index) {
		assertThat(index.get("11.o").tokens().serialize()).isEqualTo("");
		assertThat(index.get("22.o").tokens().serialize()).isEqualTo("name=jose");
	}

	@Test
	public void should_navigate_subject_structure() throws IOException {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.on("11.o").set("value", "1").commit();
			index.on("11.o/22.p").set("value", "2").commit();
			index.on("11.o/22.p/33.q").set("value", "3").commit();

			assertThat(index.subjects(Any).roots().size()).isEqualTo(1);
			assertThat(index.subjects(Any).all().size()).isEqualTo(3);
			assertThat(index.subjects("o").all().size()).isEqualTo(1);
			assertThat(index.subjects("p").all().size()).isEqualTo(1);
			assertThat(index.subjects("p").all().get(0).path()).isEqualTo("11.o/22.p");
			assertThat(index.subjects(Any).roots().get(0)).isEqualTo(Subject.of("11.o"));
			assertThat(index.subjects(Any).roots().get(0).children().get(0)).isEqualTo(Subject.of("11.o/22.p"));
			assertThat(index.get("11.o").isNull()).isFalse();
			assertThat(index.get("11.o").parent().isNull()).isTrue();
			assertThat(index.get("12.o").isNull()).isTrue();
			assertThat(index.get("11.o/22.p").parent()).isEqualTo(index.get("11.o"));
			assertThat(index.get("11.o/22.p").children()).containsExactly(index.get("11.o/22.p/33.q"));
			assertThat(index.get("11.o/22.p").children().get(0).path()).isEqualTo("11.o/22.p/33.q");
			assertThat(index.get("11.o/22.p/33.q").parent().parent()).isEqualTo(Subject.of("11.o"));
			assertThat(index.subjects(Any).with("value", "1").roots().size()).isEqualTo(1);
			assertThat(index.subjects("o").with("value", "1").roots().size()).isEqualTo(1);
			assertThat(index.subjects("p").with("value", "1").roots().size()).isEqualTo(0);
			assertThat(index.subjects("p").with("value", "2").all().size()).isEqualTo(1);
			assertThat(index.subjects("p").with("value", "2").all().get(0)).isEqualTo(Subject.of("11.o/22.p"));
			assertThat(index.subjects(Any).with("value", "2").roots().isEmpty()).isTrue();
		}
	}

	@Test
	public void should_support_drop_documents_and_conditional_query() throws IOException {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.on("11.o")
					.set("name", "jose")
					.set("team", "first")
					.commit();
			index.on("22.o")
					.set("name", "luis")
					.set("team", "first")
					.commit();
			index.drop("11.o");
			assertThat(index.subjects(Any).roots().serialize()).isEqualTo("22.o");
		}
		try (SubjectIndex index = new SubjectIndex(file)) {
			assertThat(index.subjects(Any).roots().serialize()).isEqualTo("22.o");
		}
	}

	@Test
	public void should_support_unset_tokens_and_conditional_query() throws IOException {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.on("123456.model")
				.set("description","simulation")
				.set("user", "mcaballero@gmail.com")
				.commit();
			index.on("654321.model")
				.set("t","simulation")
				.set("user", "mcaballero@gmail.com")
				.set("user", "josejuan@gmail.com")
				.commit();
			index.on("654321.model")
				.unset("t","simulation")
				.commit();
			assertThat(index.subjects(Any).roots().serialize()).isEqualTo("123456.model\n654321.model");
			assertThat(index.subjects(Any).roots().serialize()).isEqualTo("123456.model\n654321.model");
			assertThat(index.subjects(Any).with("user","mcaballero@gmail.com").roots().serialize()).isEqualTo("123456.model\n654321.model");
			assertThat(index.subjects(Any).with("description","simulation").roots().serialize()).isEqualTo("123456.model");
			assertThat(index.subjects(Any).with("user", "josejuan@gmail.com").roots().serialize()).isEqualTo("654321.model");
			assertThat(index.subjects(Any).without("description","simulation").roots().serialize()).isEqualTo("654321.model");
			assertThat(index.subjects(Any).without("user", "josejuan@gmail.com").roots().serialize()).isEqualTo("123456.model");
		}
	}

	@Test
	public void should_get_tokens_of_subject() throws IOException {
		File file = File.createTempFile("file", ".inx");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.on("11.o")
				.set("name", "jose")
				.commit();
			index.on("123456.model")
				.set("t", "simulation")
				.set("user", "mcaballero@gmail.com")
				.set("user", "josejuan@gmail.com")
				.set("project", "ulpgc")
				.set("t", "simulation")
				.set("team", "ulpgc")
				.commit();
			index.on("123456.model")
				.unset("team", "ulpgc")
				.commit();
			assertThat(index.get("11.o").tokens().serialize()).isEqualTo("name=jose");
			assertThat(index.get("123456.model").tokens().serialize()).isEqualTo("t=simulation\nuser=mcaballero@gmail.com\nuser=josejuan@gmail.com\nproject=ulpgc");
		}
	}

	@Test
	public void should_fin_using_contain_and_fit_filters() throws IOException {
		File file = File.createTempFile("file", ".inx");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.on("123456.model")
					.set("description", "simulation")
					.set("user", "mcaballero@gmail.com")
					.set("user", "josejuan@gmail.com")
					.set("project", "ulpgc")
					.set("description", "simulation")
					.set("team", "ulpgc")
					.set("access", ".*@ulpgc\\.es")
					.commit();
			assertThat(index.subjects(Any).where("team").contains("ulpgc").serialize()).isEqualTo("123456.model");

			index.on("123456.model")
					.unset("team", "ulpgc")
					.commit();
			assertThat(index.subjects(Any).where("description", "user").contains("gmail").serialize()).isEqualTo("123456.model");
			assertThat(index.subjects(Any).where("description").contains("sim").serialize()).isEqualTo("123456.model");
			assertThat(index.subjects(Any).where("description").contains("xxx").serialize()).isEqualTo("");
			assertThat(index.subjects(Any).where("team").contains("ulpgc").serialize()).isEqualTo("");
			assertThat(index.subjects(Any).where("access").matches("jose@gmail.com").serialize()).isEqualTo("");
			assertThat(index.subjects(Any).where("access").matches("jose@ulpgc.es").serialize()).isEqualTo("123456.model");
		}
	}

	@Test
	public void should_index_subjects_with_tokens_and_support_deletion() throws IOException {
		File file = File.createTempFile("subjects", ".inx");

		try (SubjectIndex index = new SubjectIndex(file)) {
			index.on("P001.model")
					.set("name", "AI Research")
					.set("lead", "alice@example.com")
					.set("status", "active")
					.commit();

			index.on("P001.model/E001.experiment")
					.set("name", "Language Model Evaluation")
					.set("dataset", "OpenQA")
					.commit();

			index.on("P001.model/E002.experiment")
					.set("name", "Graph Alignment")
					.set("dataset", "DBpedia")
					.set("status", "archived")
					.commit();

			index.on("P001.model/E002.experiment")
					.unset("status", "archived")
					.commit();

			index.drop("P001.model/E001.experiment");

			assertThat(index.get("P001.model").isNull()).isFalse();
			assertThat(index.get("P001.model/E001.experiment").isNull()).isTrue(); // fue borrado
			assertThat(index.get("P001.model/E002.experiment").isNull()).isFalse();

			assertThat(index.get("P001.model/E002.experiment").tokens()
					.serialize()).doesNotContain("status=archived");

			assertThat(index.get("P001.model").children()).hasSize(1);
			assertThat(index.get("P001.model").children().get(0).name()).isEqualTo("E002");

			assertThat(index.subjects("model").with("name", "AI Research").all().serialize()).contains("P001.model");
		}
	}

	@Test
	public void should_test_load() throws Exception {
		File file = File.createTempFile("subjects", ".inx");
		try (SubjectIndex index = load(file)) {
			assertThat(index.subjects(Any).roots().size()).isEqualTo(25);
			Subjects subjects = index.subjects(Any).all();
			assertThat(subjects.size()).isEqualTo(125);
			assertThat(index.tokens().size()).isEqualTo(55);
			int nonRootDeleted = 0;
			int rootDeleted = 0;
			for (int i = 0; i < 125; i += 3) {
				Subject subject = subjects.get(i);
				index.drop(subject);
				if (subject.isRoot()) rootDeleted++; else nonRootDeleted++;
			}
			assertThat(nonRootDeleted).isEqualTo(33);
			assertThat(rootDeleted).isEqualTo(9);
			assertThat(index.subjects(Any).all().size()).isEqualTo(83);
			assertThat(index.subjects(Any).roots().size()).isEqualTo(16);
			assertThat(index.tokens().size()).isEqualTo(37);
		}
		try (SubjectIndex index = new SubjectIndex(file)) {
			assertThat(index.subjects(Any).all().size()).isEqualTo(83);
			assertThat(index.subjects(Any).roots().size()).isEqualTo(16);
			assertThat(index.tokens().size()).isEqualTo(37);
		}
	}

	private SubjectIndex load(File file) throws Exception {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream()))) {
			SubjectIndex index = new SubjectIndex(file);
			while (true) {
				String line = reader.readLine();
				if (line == null) break;
				load(line, index);
			}
			return index;
		}
	}

	private void load(String line, SubjectIndex index) {
		String[] split = line.split(" with ", 2);
		if (split.length != 2) return;

		String path = split[0].trim();
		String[] tokens = split[1].split("\\|");

		SubjectIndex.Indexing indexing = index.on(path);
		for (String token : tokens)
			indexing.set(Token.of(token));
		indexing.commit();
	}

	private static InputStream inputStream() {
		return SubjectIndex_.class.getClassLoader().getResourceAsStream("subjects.txt");
	}
}

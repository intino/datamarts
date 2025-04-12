package test;

import org.junit.Test;
import systems.intino.datamarts.subjectindex.SubjectIndex;
import systems.intino.datamarts.subjectindex.model.Subject;
import systems.intino.datamarts.subjectindex.model.Subjects;
import systems.intino.datamarts.subjectindex.model.Token;

import java.io.*;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectIndex_ {

	@Test
	public void should_support_index_subject_and_conditional_query() throws IOException {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex tree = new SubjectIndex(file)) {
			tree.create("11", "o").update().put("name", "jose").commit();
			check(tree);
		}
		try (SubjectIndex tree = new SubjectIndex(file)) {
			tree.create("11", "o").update()
					.put("name", "jose")
					.commit();
			check(tree);
		}
	}

	private static void check(SubjectIndex tree)  {
		assertThat(tree.subjects().roots().serialize()).isEqualTo("11.o");
		assertThat(tree.subjects().with("name", "jose").roots().serialize()).isEqualTo("11.o");
		assertThat(tree.subjects().without("name", "jose").roots().serialize()).isEqualTo("");
		assertThat(tree.subjects().without("name", "mario").roots().serialize()).isEqualTo("11.o");
		assertThat(tree.subjects().without("name", "mario").roots().filter(a -> a.is("o")).serialize()).isEqualTo("11.o");
		assertThat(tree.subjects().without("name", "mario").roots().filter(a -> a.is("user")).serialize()).isEqualTo("");
	}

	@Test
	public void should_support_rename_subjects() throws IOException {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex tree = new SubjectIndex(file)) {
			tree.create("11", "o").update().put("name", "jose").commit();
			tree.create("11", "o").update().rename("22.o").commit();
			checkRename(tree);
		}
		try (SubjectIndex tree = new SubjectIndex(file)) {
			checkRename(tree);
		}
	}

	private static void checkRename(SubjectIndex tree)  {
		assertThat(tree.get("11","o")).isNull();
		assertThat(tree.get("22", "o").tokens().serialize()).isEqualTo("name=jose");
	}

	@Test
	public void should_support_set_and_del_tokens() throws IOException {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex tree = new SubjectIndex(file)) {
			tree.create("11", "o").update().set("name", "jose").commit();
			assertThat(tree.get("11","o").tokens().serialize()).isEqualTo("name=jose");
		}
		try (SubjectIndex tree = new SubjectIndex(file)) {
			assertThat(tree.get("11","o").tokens().serialize()).isEqualTo("name=jose");
			tree.create("11", "o").update().set("name", "mario").commit();
			assertThat(tree.get("11","o").tokens().serialize()).isEqualTo("name=mario");
		}
		try (SubjectIndex tree = new SubjectIndex(file)) {
			assertThat(tree.get("11","o").tokens().serialize()).isEqualTo("name=mario");
			tree.create("11", "o").update().del("name").commit();
			assertThat(tree.get("11","o").tokens().serialize()).isEqualTo("");
		}
		try (SubjectIndex tree = new SubjectIndex(file)) {
			assertThat(tree.get("11","o").tokens().serialize()).isEqualTo("");
			tree.create("11", "o").update().put("name", "mario").commit();
			tree.create("11", "o").update().put("name", "jose").commit();
			assertThat(tree.get("11","o").tokens().serialize()).isEqualTo("name=jose\nname=mario");
		}
		try (SubjectIndex tree = new SubjectIndex(file)) {
			assertThat(tree.get("11","o").tokens().serialize()).isEqualTo("name=jose\nname=mario");
			tree.create("11", "o").update().del("name").commit();
			assertThat(tree.get("11","o").tokens().serialize()).isEqualTo("");
		}
		try (SubjectIndex tree = new SubjectIndex(file)) {
			assertThat(tree.get("11","o").tokens().serialize()).isEqualTo("");
		}
	}

	@Test
	public void should_navigate_subject_structure() throws IOException {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex tree = new SubjectIndex(file)) {
			Subject s1 = tree.create("11","o");
			s1.update().put("value", "1").commit();
			Subject s2 = s1.create("22", "p");
			s2.update().put("value", "2").commit();
			Subject s3 = s2.create("33", "q");
			s3.update().put("value", "3").commit();
			Subject s4 = s2.create("44", "q");
			s4.update().put("value", "4").commit();
			s4.drop();

			assertThat(tree.subjects().roots().size()).isEqualTo(1);
			assertThat(tree.subjects().all().size()).isEqualTo(3);
			assertThat(tree.subjects("o").all().size()).isEqualTo(1);
			assertThat(tree.subjects("p").all().size()).isEqualTo(1);
			assertThat(tree.subjects("p").all().get(0).path()).isEqualTo("11.o/22.p");
			assertThat(tree.subjects().roots().get(0)).isEqualTo(Subject.of("11.o"));
			assertThat(tree.subjects().roots().get(0).children().get(0)).isEqualTo(Subject.of("11.o/22.p"));
			assertThat(tree.get("11","o").isNull()).isFalse();
			assertThat(tree.get("11","o").parent().isNull()).isTrue();
			assertThat(tree.get("12" ,"o")).isNull();
			assertThat(tree.get("11.o/22.p").parent()).isEqualTo(tree.get("11","o"));
			assertThat(tree.get("11.o/22.p").children()).containsExactly(tree.get("11.o/22.p/33.q"));
			assertThat(tree.get("11.o/22.p").children().get(0).path()).isEqualTo("11.o/22.p/33.q");
			assertThat(tree.get("11.o/22.p/33.q").parent().parent()).isEqualTo(Subject.of("11.o"));
			assertThat(tree.subjects().with("value", "1").roots().size()).isEqualTo(1);
			assertThat(tree.subjects("o").with("value", "1").roots().size()).isEqualTo(1);
			assertThat(tree.subjects("p").with("value", "1").roots().size()).isEqualTo(0);
			assertThat(tree.subjects("p").with("value", "2").all().size()).isEqualTo(1);
			assertThat(tree.subjects("p").with("value", "2").all().get(0)).isEqualTo(Subject.of("11.o/22.p"));
			assertThat(tree.subjects().with("value", "2").roots().isEmpty()).isTrue();
		}
	}

	@Test
	public void should_support_drop_subjects() throws IOException {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex tree = new SubjectIndex(file)) {
			Subject subject = tree.create("11", "o");
			subject.update()
					.put("name", "jose")
					.put("team", "first")
					.commit();
			tree.create("22","o").update()
					.put("name", "luis")
					.put("team", "first")
					.commit();
			tree.get("11.o").drop();
			assertThat(tree.subjects().roots().serialize()).isEqualTo("22.o");
			assertThat(tree.tokens().serialize()).isEqualTo("team=first\nname=luis");
		}
		try (SubjectIndex tree = new SubjectIndex(file)) {
			assertThat(tree.subjects().roots().serialize()).isEqualTo("22.o");
		}
	}

	@Test
	public void should_support_unset_tokens_and_conditional_query() throws IOException {
		File file = File.createTempFile("subject", ".inx");
		try (SubjectIndex tree = new SubjectIndex(file)) {
			tree.create("123456", "model").update()
					.put("description","simulation")
					.put("user", "mcaballero@gmail.com")
					.commit();
			tree.create("654321","model").update()
					.put("t","simulation")
					.put("user", "mcaballero@gmail.com")
					.put("user", "josejuan@gmail.com")
					.commit();
			tree.create("654321","model").update()
					.del("t","simulation")
					.commit();
			assertThat(tree.subjects().roots().serialize()).isEqualTo("123456.model\n654321.model");
			assertThat(tree.subjects().roots().serialize()).isEqualTo("123456.model\n654321.model");
			assertThat(tree.subjects().with("user","mcaballero@gmail.com").roots().serialize()).isEqualTo("123456.model\n654321.model");
			assertThat(tree.subjects().with("description","simulation").roots().serialize()).isEqualTo("123456.model");
			assertThat(tree.subjects().with("user", "josejuan@gmail.com").roots().serialize()).isEqualTo("654321.model");
			assertThat(tree.subjects().without("description","simulation").roots().serialize()).isEqualTo("654321.model");
			assertThat(tree.subjects().without("user", "josejuan@gmail.com").roots().serialize()).isEqualTo("123456.model");
		}
	}

	@Test
	public void should_get_tokens_of_subject() throws IOException {
		File file = File.createTempFile("file", ".inx");
		try (SubjectIndex tree = new SubjectIndex(file)) {
			tree.create("11", "o").update()
					.put("name", "jose")
					.commit();
			tree.create("123456", "model").update()
					.put("t", "simulation")
					.put("user", "mcaballero@gmail.com")
					.put("user", "josejuan@gmail.com")
					.put("project", "ulpgc")
					.put("t", "simulation")
					.put("team", "ulpgc")
					.commit();
			tree.create("123456", "model").update()
					.del("team", "ulpgc")
					.commit();
			assertThat(tree.get("11","o").tokens().serialize()).isEqualTo("name=jose");
			assertThat(tree.get("123456", "model").tokens().serialize()).isEqualTo("t=simulation\nuser=mcaballero@gmail.com\nuser=josejuan@gmail.com\nproject=ulpgc");
		}
	}

	@Test
	public void should_fin_using_contain_and_fit_filters() throws IOException {
		File file = File.createTempFile("file", ".inx");
		try (SubjectIndex tree = new SubjectIndex(file)) {
			tree.create("123456", "model").update()
					.put("description", "simulation")
					.put("user", "josejuan@gmail.com")
					.put("project", "ulpgc")
					.put("description", "simulation")
					.put("team", "ulpgc.es")
					.put("access", ".*@ulpgc\\.es")
					.commit();
			tree.create("654321","model").update()
					.put("description", "simulation")
					.put("user", "mcaballero@gmail.com")
					.put("project", "ulpgc")
					.commit();

			tree.create("123456", "model").update()
					.del("team", "ulpgc")
					.commit();
			assertThat(tree.subjects().where("team", "project").contains("ulpgc es").serialize()).isEqualTo("123456.model");
			assertThat(tree.subjects().where("description", "user").contains("gmail").serialize()).isEqualTo("123456.model\n654321.model");
			assertThat(tree.subjects().where("description").contains("sim").serialize()).isEqualTo("123456.model\n654321.model");
			assertThat(tree.subjects().where("description").contains("xxx").serialize()).isEqualTo("");
			assertThat(tree.subjects().where("team").contains("ulpgc").serialize()).isEqualTo("123456.model");
			assertThat(tree.subjects().where("access").matches("jose@gmail.com").serialize()).isEqualTo("");
			assertThat(tree.subjects().where("access").matches("jose@ulpgc.es").serialize()).isEqualTo("123456.model");
		}
	}

	@Test
	public void should_index_subjects_with_tokens_and_support_deletion() throws IOException {
		File file = File.createTempFile("subjects", ".inx");

		try (SubjectIndex tree = new SubjectIndex(file)) {
			tree.create("P001.model").update()
					.put("name", "AI Research")
					.put("lead", "alice@example.com")
					.put("status", "active")
					.commit();

			tree.create("P001.model/E001.experiment").update()
					.put("name", "Language Model Evaluation")
					.put("dataset", "OpenQA")
					.commit();

			tree.create("P001.model/E002.experiment").update()
					.put("name", "Graph Alignment")
					.put("dataset", "DBpedia")
					.put("status", "archived")
					.commit();

			tree.create("P001.model/E002.experiment").update()
					.del("status", "archived")
					.commit();

			tree.get("P001.model/E001.experiment").drop();


			tree.create("P001.model/E002.experiment").update();

			assertThat(tree.get("P001.model").isNull()).isFalse();
			assertThat(tree.get("P001.model/E001.experiment")).isNull();

			assertThat(tree.get("P001.model/E002.experiment").tokens()
					.serialize()).doesNotContain("status=archived");

			assertThat(tree.get("P001.model").children()).hasSize(1);
			assertThat(tree.get("P001.model").children().get(0).name()).isEqualTo("E002");

			assertThat(tree.subjects("model").with("name", "AI Research").all().serialize()).contains("P001.model");
		}
	}

	@Test
	public void should_test_load() throws Exception {
		File file = File.createTempFile("subjects", ".inx");
		try (SubjectIndex tree = load(file)) {
			assertThat(tree.subjects().roots().size()).isEqualTo(25);
			Subjects subjects = tree.subjects().all();
			assertThat(subjects.size()).isEqualTo(125);
			assertThat(tree.tokens().size()).isEqualTo(55);
			Map<Subject, String> statuses = tree.subjects("model").collect("status").toMap();
			int nonRootDeleted = 0;
			int rootDeleted = 0;
			for (int i = 0; i < 125; i += 3) {
				Subject subject = subjects.get(i);
				subject.drop();
				if (subject.isRoot()) rootDeleted++; else nonRootDeleted++;
			}
			assertThat(nonRootDeleted).isEqualTo(33);
			assertThat(rootDeleted).isEqualTo(9);
			assertThat(tree.subjects().all().size()).isEqualTo(56);
			assertThat(tree.subjects().roots().size()).isEqualTo(16);
			assertThat(tree.tokens().size()).isEqualTo(37);
		}
		try (SubjectIndex tree = new SubjectIndex(file)) {
			assertThat(tree.subjects().all().size()).isEqualTo(56);
			assertThat(tree.subjects().roots().size()).isEqualTo(16);
			assertThat(tree.tokens().size()).isEqualTo(37);
		}
	}

	private SubjectIndex load(File file) throws Exception {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream()))) {
			SubjectIndex tree = new SubjectIndex(file);
			while (true) {
				String line = reader.readLine();
				if (line == null) break;
				load(line, tree);
			}
			return tree;
		}
	}

	private void load(String line, SubjectIndex tree)  {
		String[] split = line.split(" with ", 2);
		if (split.length != 2) return;

		String path = split[0].trim();
		String[] tokens = split[1].split("\\|");

		Subject.Transaction transaction = tree.create(path).update();
		for (String token : tokens)
			transaction.put(Token.of(token));
		transaction.commit();
	}

	private static InputStream inputStream() {
		return SubjectIndex_.class.getClassLoader().getResourceAsStream("subjects.txt");
	}
}

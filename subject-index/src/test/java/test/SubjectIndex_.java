package test;

import org.junit.Test;
import systems.intino.alexandria.datamarts.subjectmap.SubjectIndex;
import systems.intino.alexandria.datamarts.subjectmap.model.Subject;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static systems.intino.alexandria.datamarts.subjectmap.model.Subject.Any;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectIndex_ {

	@Test
	public void should_support_index_subject_and_conditional_query() throws IOException {
		File file = File.createTempFile("subject", ".iam");
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
		File file = File.createTempFile("subject", ".iam");
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
		assertThat(index.tokens().of(index.get("11.o")).serialize()).isEqualTo("");
		assertThat(index.tokens().of(index.get("22.o")).serialize()).isEqualTo("name=jose");
	}

	@Test
	public void should_navigate_subject_structure() throws IOException {
		File file = File.createTempFile("subject", ".iam");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.on("11.o").set("value", "1").commit();
			index.on("11.o/22.p").set("value", "2").commit();
			index.on("11.o/22.p/33.q").set("value", "3").commit();

			assertThat(index.subjects(Any).roots().size()).isEqualTo(1);
			assertThat(index.subjects(Any).roots().get(0)).isEqualTo(Subject.of("11.o"));
			assertThat(index.subjects(Any).roots().get(0).children().getFirst()).isEqualTo(Subject.of("11.o/22.p"));
			assertThat(index.get("11.o").isNull()).isFalse();
			assertThat(index.get("11.o").parent().isNull()).isTrue();
			assertThat(index.get("12.o").isNull()).isTrue();
			assertThat(index.get("11.o/22.p").parent()).isEqualTo(index.get("11.o"));
			assertThat(index.get("11.o/22.p").children()).containsExactly(index.get("11.o/22.p/33.q"));
			assertThat(index.get("11.o/22.p").children().getFirst().path()).isEqualTo("11.o/22.p/33.q");
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
		File file = File.createTempFile("subject", ".iam");
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
		File file = File.createTempFile("subject", ".iam");
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
		File file = File.createTempFile("file", ".iam");
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
			assertThat(index.tokens().of("11.o").serialize()).isEqualTo("name=jose");
			assertThat(index.tokens().of("123456.model").serialize()).isEqualTo("t=simulation\nuser=mcaballero@gmail.com\nuser=josejuan@gmail.com\nproject=ulpgc");
		}
	}

	@Test
	public void should_fin_using_contain_and_fit_filters() throws IOException {
		File file = File.createTempFile("file", ".iam");
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
			//assertThat(index.subjects().where("team").contains("ulpgc").serialize()).isEqualTo("123456.model");

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
}

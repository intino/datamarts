package test;

import org.junit.Test;
import systems.intino.alexandria.datamarts.subjectmap.SubjectIndex;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class SubjectRegistry_ {

	@Test
	public void should_support_set_tokens_and_conditional_query() throws IOException {
		File file = File.createTempFile("subject", ".iam");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.on("111111", "o")
					.set("name", "jose")
					.commit();
			check(index);
		}
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.on("111111", "o")
					.set("name", "jose")
					.commit();
			check(index);
		}
	}

	private static void check(SubjectIndex index) {
		assertThat(index.subjects().toSet().serialize()).isEqualTo("111111:o");
		assertThat(index.subjects().with("name", "jose").toSet().serialize()).isEqualTo("111111:o");
		assertThat(index.subjects().without("name", "jose").toSet().serialize()).isEqualTo("");
		assertThat(index.subjects().without("name", "mario").toSet().serialize()).isEqualTo("111111:o");
		assertThat(index.subjects().without("name", "mario").toSet().filter(a -> a.is("o")).serialize()).isEqualTo("111111:o");
		assertThat(index.subjects().without("name", "mario").toSet().filter(a -> a.is("user")).serialize()).isEqualTo("");
	}

	@Test
	public void should_support_drop_documents_and_conditional_query() throws IOException {
		File file = File.createTempFile("subject", ".iam");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.on("111111", "o")
					.set("name", "jose")
					.commit();
			index.drop("111111", "o");
			assertThat(index.subjects().toSet().serialize()).isEqualTo("");
		}
		try (SubjectIndex index = new SubjectIndex(file)) {
			assertThat(index.subjects().toSet().serialize()).isEqualTo("");
		}
	}

	@Test
	public void should_support_unset_tokens_and_conditional_query() throws IOException {
		File file = File.createTempFile("subject", ".iam");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.on("123456", "model")
				.set("description","simulation")
				.set("user", "mcaballero@gmail.com")
				.commit();
			index.on("654321", "model")
				.set("t","simulation")
				.set("user", "mcaballero@gmail.com")
				.set("user", "josejuan@gmail.com")
				.commit();
			index.on("654321", "model")
				.unset("t","simulation")
				.commit();
			assertThat(index.subjects().toSet().serialize()).isEqualTo("123456:model\n654321:model");
			assertThat(index.subjects().toSet().serialize()).isEqualTo("123456:model\n654321:model");
			assertThat(index.subjects().with("user","mcaballero@gmail.com").toSet().serialize()).isEqualTo("123456:model\n654321:model");
			assertThat(index.subjects().with("description","simulation").toSet().serialize()).isEqualTo("123456:model");
			assertThat(index.subjects().with("user", "josejuan@gmail.com").toSet().serialize()).isEqualTo("654321:model");
			assertThat(index.subjects().without("description","simulation").toSet().serialize()).isEqualTo("654321:model");
			assertThat(index.subjects().without("user", "josejuan@gmail.com").toSet().serialize()).isEqualTo("123456:model");
		}
	}

	@Test
	public void should_get_tokens_of_subject() throws IOException {
		File file = File.createTempFile("file", ".iam");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.on("111111","o")
				.set("name", "jose")
				.commit();
			index.on("123456", "model")
				.set("t", "simulation")
				.set("user", "mcaballero@gmail.com")
				.set("user", "josejuan@gmail.com")
				.set("project", "ulpgc")
				.set("t", "simulation")
				.set("team", "ulpgc")
				.commit();
			index.on("123456", "model")
				.unset("team", "ulpgc")
				.commit();
			assertThat(index.tokens().of("111111", "o").serialize()).isEqualTo("name=jose");
			assertThat(index.tokens().of("123456", "model").serialize()).isEqualTo("t=simulation\nuser=mcaballero@gmail.com\nuser=josejuan@gmail.com\nproject=ulpgc");
		}
	}

	@Test
	public void should_fin_using_contain_and_fit_filters() throws IOException {
		File file = File.createTempFile("file", ".iam");
		try (SubjectIndex index = new SubjectIndex(file)) {
			index.on("123456", "model")
					.set("description", "simulation")
					.set("user", "mcaballero@gmail.com")
					.set("user", "josejuan@gmail.com")
					.set("project", "ulpgc")
					.set("description", "simulation")
					.set("team", "ulpgc")
					.set("access", ".*@ulpgc\\.es")
					.commit();
			assertThat(index.subjects().where("team").contains("ulpgc").serialize()).isEqualTo("123456:model");

			index.on("123456", "model")
					.unset("team", "ulpgc")
					.commit();
			assertThat(index.subjects().where("description").contains("sim").serialize()).isEqualTo("123456:model");
			assertThat(index.subjects().where("description").contains("xxx").serialize()).isEqualTo("");
			assertThat(index.subjects().where("team").contains("ulpgc").serialize()).isEqualTo("");
			assertThat(index.subjects().where("access").matches("jose@gmail.com").serialize()).isEqualTo("");
			assertThat(index.subjects().where("access").matches("jose@ulpgc.es").serialize()).isEqualTo("123456:model");
		}
	}
}

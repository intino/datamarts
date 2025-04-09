package test;

import org.junit.Test;
import systems.intino.alexandria.datamarts.anchormap.AnchorMap;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class AnchorMap_ {

	@Test
	public void should_support_token_add_remove_and_conditional_search() throws IOException {
		File file = File.createTempFile("file", ".iam");
		try (AnchorMap map = new AnchorMap(file)) {
			map.on("111111")
					.set("name", "jose")
					.commit();
			map.on("123456", "model")
					.set("simulation")
					.set("user", "mcaballero@gmail.com")
					.commit();
			map.on("654321", "model")
					.set("simulation")
					.set("user", "mcaballero@gmail.com")
					.set("user", "josejuan@gmail.com")
					.commit();
			map.on("654321", "model")
					.unset("simulation")
					.commit();
			assertThat(map.search().execute()).containsExactly("111111:document");
			assertThat(map.search().with("name","jose").execute()).containsExactly("111111:document");
			assertThat(map.search().without("name","mario").execute()).containsExactly("111111:document");
			assertThat(map.search("model").execute()).containsExactly("123456:model", "654321:model");
			assertThat(map.search("model").execute()).containsExactly("123456:model", "654321:model");
			assertThat(map.search("model").with("user","mcaballero@gmail.com").execute()).containsExactly("123456:model", "654321:model");
			assertThat(map.search("model").with("simulation").execute()).containsExactly("123456:model");
			assertThat(map.search("model").without("simulation").execute()).containsExactly("654321:model");
			assertThat(map.search("model").with("user", "josejuan@gmail.com").execute()).containsExactly("654321:model");
			assertThat(map.search("model").without("user", "josejuan@gmail.com").execute()).containsExactly("123456:model");
		}
	}

	@Test
	public void should_set_and_unset_tokens() throws IOException {
		File file = File.createTempFile("file", ".iam");
		try (AnchorMap map = new AnchorMap(file)) {
			map.on("111111")
				.set("name", "jose")
				.commit();
			map.on("123456", "model")
				.set("simulation")
				.set("user", "mcaballero@gmail.com")
				.set("user", "josejuan@gmail.com")
				.set("project", "ulpgc")
				.set("simulation")
				.set("team", "ulpgc")
				.commit();
			map.on("123456", "model")
				.unset("team", "ulpgc")
				.commit();
			assertThat(map.get("111111")).containsExactly("name=jose");
			assertThat(map.get("123456", "model")).containsExactly("simulation", "user=mcaballero@gmail.com", "user=josejuan@gmail.com", "project=ulpgc");
		}
	}
}

package tests;

import systems.intino.alexandria.datamarts.model.Point;
import org.junit.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class Point_ {
	@Test
	public void should_generate_name() {
		assertThat(new Point<>(0, Instant.parse("2025-01-01T01:02:03Z"), "4").toString()).isEqualTo("[20250101010203=4]");
	}
}

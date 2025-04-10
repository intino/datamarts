package test;

import org.junit.Test;
import systems.intino.alexandria.datamarts.subjectmap.model.Token;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class Token_ {
	@Test
	public void should_trim_key_and_value() {
		assertThat(Token.of(" x = 20").key()).isEqualTo("x");
		assertThat(Token.of(" x = 20").value()).isEqualTo("20");
	}
}

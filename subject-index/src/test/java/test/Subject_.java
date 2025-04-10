package test;

import org.junit.Test;
import systems.intino.alexandria.datamarts.subjectmap.model.Subject;
import systems.intino.alexandria.datamarts.subjectmap.model.Subjects;
import systems.intino.alexandria.datamarts.subjectmap.model.Token;
import systems.intino.alexandria.datamarts.subjectmap.model.Tokens;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class Subject_ {
	@Test
	public void should_navigate_to_parent() {
		Subject subject = Subject.of(" a / b.release  ");
		assertThat(subject.identifier()).isEqualTo("b.release");
		assertThat(subject.name()).isEqualTo("b");
		assertThat(subject.type()).isEqualTo("release");
		assertThat(subject.parent().identifier()).isEqualTo("a");
		assertThat(subject.parent().name()).isEqualTo("a");
		assertThat(subject.parent().type()).isEqualTo("");
		assertThat(subject.parent().parent().isNull()).isTrue();
	}

	@Test
	public void should_navigate_to_children() {
		Subject subject = Subject.of(" a.model ", context());
		assertThat(subject.children()).containsExactly(Subject.of("a.model/b.release"));
		assertThat(subject.children().get(0).children().get(0)).isEqualTo(Subject.of("a.model/b.release/b.release"));
	}

	@Test
	public void should_write_in_system_error_when_subject_context_is_not_defined() {
		PrintStream originalErr = System.err;
		ByteArrayOutputStream errContent = new ByteArrayOutputStream();
		System.setErr(new PrintStream(errContent));
		try {
			Subject.of("a.model").tokens();
			String output = errContent.toString().trim();
			assertThat(output).contains("Context is not defined for 'a.model'");

		} finally {
			System.setErr(originalErr);
		}
	}

	private Subject.Context context() {
		return new Subject.Context(this::childrenOf, this::tokensOf);
	}

	private Tokens tokensOf(Subject subject) {
		return new Tokens(List.of(new Token("email", "data@gmail.com")));
	}

	private Subjects childrenOf(Subject subject) {
		return new Subjects(List.of(Subject.of(subject.path() + "/" + "b.release", subject.context())));
	}
}

package test;

import org.junit.Test;
import systems.intino.alexandria.datamarts.subjectmap.model.Subject;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("NewClassNamingConvention")
public class Subject_ {
	@Test
	public void should_navigate_to_parent() {
		Subject subject = Subject.of("a.model/b.release");
		assertThat(subject.identifier()).isEqualTo("b.release");
		assertThat(subject.name()).isEqualTo("b");
		assertThat(subject.type()).isEqualTo("release");
		assertThat(subject.parent().identifier()).isEqualTo("a.model");
		assertThat(subject.parent().name()).isEqualTo("a");
		assertThat(subject.parent().type()).isEqualTo("model");
		assertThat(subject.parent().parent().isNull()).isTrue();
	}

	@Test
	public void should_navigate_to_children() {
		Subject subject = Subject.of("a.model", this::childrenOf);
		assertThat(subject.children()).containsExactly(Subject.of("a.model/b.release"));
		assertThat(subject.children().getFirst().children().getFirst()).isEqualTo(Subject.of("a.model/b.release/b.release"));
	}

	private List<Subject> childrenOf(Subject subject) {
		return List.of(Subject.of(subject.path() + "/" + "b.release", subject.childrenLookup()));
	}
}

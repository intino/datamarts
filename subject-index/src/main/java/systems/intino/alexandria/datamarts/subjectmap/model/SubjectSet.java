package systems.intino.alexandria.datamarts.subjectmap.model;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public record SubjectSet(List<Subject> subjects) implements Iterable<Subject> {
	public String serialize() {
		return subjects.stream()
				.filter(Objects::nonNull)
				.map(Subject::toString)
				.collect(Collectors.joining("\n"));
	}

	public SubjectSet filter(Predicate<Subject> predicate) {
		return new SubjectSet(subjects.stream().filter(predicate).toList());
	}

	public int size() {
		return subjects.size();
	}

	public boolean isEmpty() {
		return subjects.isEmpty();
	}

	@Override
	public Iterator<Subject> iterator() {
		return subjects.iterator();
	}
}

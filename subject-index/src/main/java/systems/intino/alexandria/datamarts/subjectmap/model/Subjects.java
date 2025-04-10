package systems.intino.alexandria.datamarts.subjectmap.model;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public record Subjects(List<Subject> subjects) implements Iterable<Subject> {

	public boolean isEmpty() {
		return subjects.isEmpty();
	}

	public int size() {
		return subjects.size();
	}

	public Subject get(int index) {
		return subjects.get(index);
	}

	public Subjects filter(Predicate<Subject> predicate) {
		return new Subjects(subjects.stream().filter(predicate).toList());
	}

	public Stream<Subject> stream() {
		return subjects.stream();
	}

	@Override
	public Iterator<Subject> iterator() {
		return subjects.iterator();
	}

	public String serialize() {
		return subjects.stream()
				.filter(Objects::nonNull)
				.map(Subject::toString)
				.collect(Collectors.joining("\n"));
	}
}

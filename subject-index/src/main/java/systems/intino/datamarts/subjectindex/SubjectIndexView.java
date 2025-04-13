package systems.intino.datamarts.subjectindex;

import systems.intino.datamarts.subjectindex.model.Subject;
import systems.intino.datamarts.subjectindex.model.Subjects;
import systems.intino.datamarts.subjectindex.model.Tokens;
import systems.intino.datamarts.subjectindex.model.Statement;
import systems.intino.datamarts.subjectindex.view.Column;

import java.util.*;
import java.util.stream.Stream;

public class SubjectIndexView implements Iterable<Column>  {
	private final Subjects subjects;
	private final List<String> keys;

	public static Builder of(SubjectIndex subjectIndex) {
		return new Builder(subjectIndex);
	}

	private SubjectIndexView(Subjects subjects, List<String> keys) {
		this.subjects = subjects;
		this.keys = keys;
	}

	public Statement[] get(int index) {
		return statements(subjects.get(index));
	}

	public Statement[] get(int index, String key) {
		Subject subject = subjects.get(index);
		return statements(subject, key).toArray(Statement[]::new);
	}

	public int size() {
		return subjects.size();
	}

	public int columns() {
		return keys.size();
	}

	public Column column(String name) {
		return new Column(name, valuesOf(name));
	}

	private String[] valuesOf(String key) {
		return subjects.stream()
				.flatMap(s-> statements(s, key))
				.map(s->s.token().value())
				.toArray(String[]::new);
	}

	@Override
	public Iterator<Column> iterator() {
		return new Iterator<>() {
			private final Iterator<String> iterator = keys.iterator();

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public Column next() {
				return column(iterator.next());
			}
		};
	}

	private Statement[] statements(Subject subject) {
		return keys.stream()
				.flatMap(s -> statements(subject, s))
				.toArray(Statement[]::new);
	}

	private Stream<Statement> statements(Subject subject, String key) {
		return tokensOf(subject).stream()
				.filter(t->t.is(key))
				.map(t->new Statement(subject, t));
	}

	private final Map<Subject, Tokens> tokens = new HashMap<>();
	private Tokens tokensOf(Subject subject) {
		return tokens.computeIfAbsent(subject, Subject::tokens);
	}

	public static class Builder {
		private final SubjectIndex subjectIndex;
		private final List<String> types;
		private final List<String> keys;

		public Builder(SubjectIndex subjectIndex) {
			this.subjectIndex = subjectIndex;
			this.types = new ArrayList<>();
			this.keys = new ArrayList<>();
		}

		public Builder select(String type) {
			types.add(type);
			return this;
		}

		public Builder add(String key) {
			keys.add(key);
			return this;
		}

		public SubjectIndexView build() {
			Subjects subjects = subjectIndex.subjects(setOf(types)).all();
			return new SubjectIndexView(subjects, keys);
		}

		private Set<String> setOf(List<String> types) {
			return new HashSet<>(types);
		}

	}

}

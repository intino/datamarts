package systems.intino.alexandria.datamarts.subjectmap;

import systems.intino.alexandria.datamarts.subjectmap.io.Registry;
import systems.intino.alexandria.datamarts.subjectmap.io.registries.SqlRegistry;
import systems.intino.alexandria.datamarts.subjectmap.io.registries.SqliteConnection;
import systems.intino.alexandria.datamarts.subjectmap.model.*;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class SubjectIndex implements Closeable {
	private final Registry registry;
	private final Lookup<Subject> subjects;
	private final Lookup<Token> tokens;
	private final Subject subjectNull;

	public SubjectIndex(File file) {
		this(SqliteConnection.from(file));
	}

	public SubjectIndex() {
		this(SqliteConnection.inMemory());
	}

	public SubjectIndex(Connection connection) {
		this.registry = new SqlRegistry(connection);
		this.subjects = new Lookup<>(registry.subjects(), Subject::of, this::insert);
		this.tokens = new Lookup<>(registry.tokens(), Token::of, this::insert);
		this.subjectNull = new Subject("", context());
	}

	public SubjectQuery subjects(String type) {
		return new SubjectQuery() {

			@Override
			public Subjects all() {
				return subjectFilter(type).all();
			}

			@Override
			public Subjects roots() {
				return subjectFilter(type).roots();
			}

			@Override
			public AttributeFilter where(String... keys) {
				return attributeFilter(type, Set.of(keys));
			}

			@Override
			public SubjectFilter with(String key, String value) {
				return subjectFilter(type).with(key, value);
			}

			@Override
			public SubjectFilter without(String key, String value) {
				return subjectFilter(type).without(key, value);
			}
		};
	}

	public Subject get(String path) {
		return get(Subject.of(path));
	}

	public Subject get(Subject subject) {
		return subjects.contains(subject) ? wrap(subject) : subjectNull;
	}

	private Subject.Context context() {
		return new Subject.Context(this::children, this::tokens);
	}

	private Subjects children(Subject subject) {
		return new Subjects(subjects.stream()
				.filter(s -> s.parent().equals(subject))
				.toList());
	}

	private Tokens tokens(Subject subject) {
		int id = subjects.id(subject);
		return new Tokens(id < 0 ? List.of() : tokens(registry.tokensOf(id)).toList());
	}

	public Tokens tokens() {
		return new Tokens(tokens.stream().toList());
	}

	public interface SubjectQuery {
		Subjects all();
		Subjects roots();

		AttributeFilter where(String... keys);
		SubjectFilter with(String user, String mail);
		SubjectFilter without(String description, String simulation);
	}

	private AttributeFilter attributeFilter(String type, Set<String> keys) {
		return new AttributeFilter() {
			@Override
			public Subjects contains(String value) {
				List<Integer> tokens = filterContaining(value);
				return subjectSetWith(tokens);
			}

			@Override
			public Subjects matches(String value) {
				List<Integer> tokens = filterFitting(value);
				return subjectSetWith(tokens);
			}

			private Subjects subjectSetWith(List<Integer> tokens) {
				List<Subject> subjects = tokens.isEmpty() ? List.of() : subjects(registry.subjectsFilteredBy(subjectsWith(type), tokens)).toList();
				return new Subjects(subjects);
			}

			private List<Integer> filterFitting(String value) {
				return tokens(keys)
						.filter(t -> pattern(t.value()).matcher(value).matches())
						.map(tokens::id)
						.toList();
			}

			private List<Integer> filterContaining(String value) {
				return tokens(keys)
						.filter(t -> t.value().contains(value))
						.map(tokens::id)
						.toList();
			}

			private Stream<Token> tokens(Set<String> keys) {
				return tokens.stream().filter(t -> keys.contains(t.key()));
			}
		};
	}

	private SubjectFilter subjectFilter(String type) {
		return new SubjectFilter() {
			private final List<Integer> condition = new ArrayList<>();

			@Override
			public SubjectFilter with(Token token) {
				if (!tokens.contains(token)) return SubjectFilter.Empty;
				condition.add(tokens.id(token));
				return this;
			}

			@Override
			public SubjectFilter without(Token token) {
				if (tokens.contains(token))
					condition.add(-tokens.id(token));
				return this;
			}

			@Override
			public Subjects all() {
				return retrieve(s -> true);
			}

			@Override
			public Subjects roots() {
				return retrieve(s -> s.parent().isNull());
			}

			private Subjects retrieve(Predicate<Subject> predicate) {
				List<Integer> candidates = subjectsWith(type);
				List<Integer> search = registry.subjectsFilteredBy(candidates, condition);
				List<Subject> subjects = subjects(search).filter(predicate).toList();
				return new Subjects(subjects);
			}
		};
	}

	private List<Integer> subjectsWith(String type) {
		return subjects.stream()
				.filter(s -> s.is(type))
				.map(subjects::id)
				.toList();
	}

	private final Map<String, Pattern> patterns = new HashMap<>();

	private Pattern pattern(String value) {
		return patterns.computeIfAbsent(value, s -> Pattern.compile(value));
	}

	public Indexing on(String name) {
		return on(Subject.of(name));
	}

	public Indexing on(Subject subject) {
		return new Indexing() {
			private final int id = subjects.add(subject);

			@Override
			public Indexing rename(String identifier) {
				if (identifier == null || identifier.isEmpty()) return this;
				return replace(subject.identifier(identifier));
			}

			private Indexing replace(Subject subject) {
				if (subjects.contains(subject)) return this;
				subjects.set(id, subject);
				registry.rename(id, subject.toString());
				return this;
			}

			public Indexing set(Token token) {
				registry.link(id, tokens.add(token));
				return this;
			}

			@Override
			public Indexing unset(Token token) {
				registry.unlink(id, tokens.add(token));
				return this;
			}

			@Override
			public void commit() {
				registry.commit();
			}
		};
	}

	private Subject wrap(Subject subject) {
		return new Subject(subject.path(), context());
	}

	public void drop(String subject) {
		drop(Subject.of(subject));
	}

	public void drop(Subject subject) {
		if (!subjects.contains(subject)) return;
		int id = subjects.id(subject);
		tokens.remove(registry.exclusiveTokensOf(id));
		registry.drop(id);
		subjects.remove(subject);
	}


	@Override
	public void close() throws IOException {
		registry.close();
	}

	private int insert(Subject subject) {
		return registry.insertSubject(subject.toString());
	}

	private int insert(Token token) {
		return registry.insertToken(token.toString());
	}

	private Stream<Subject> subjects(List<Integer> search) {
		return search.stream().map(subjects::get).map(this::wrap);
	}

	private Stream<Token> tokens(List<Integer> tokens) {
		return tokens.stream().map(this.tokens::get);
	}

	public interface Indexing {
		Indexing rename(String identifier);

		Indexing set(Token token);

		Indexing unset(Token token);

		default Indexing set(String key, String value) {
			return set(new Token(key, value));
		}

		default Indexing unset(String key, String value) {
			return unset(new Token(key, value));
		}

		void commit();
	}

	private static class Lookup<T> {
		private final List<T> list;
		private final Function<T, Integer> idStore;
		private final Map<T, Integer> map;

		public Lookup(List<String> list, Function<String,T> deserializer, Function<T, Integer> idStore) {
			this.list = new ArrayList<>(list.stream().map(deserializer).toList());
			this.idStore = idStore;
			this.map = init(new HashMap<>());
		}

		private Map<T, Integer> init(Map<T, Integer> map) {
			for (int id = 1; id <= list.size(); id++)
				map.put(get(id), id);
			return map;
		}

		public int id(T t) {
			return contains(t) ? map.get(t) : -1;
		}

		public T get(int id) {
			return list.get(id - 1);
		}

		public boolean contains(T t) {
			return map.containsKey(t);
		}

		public int add(T t) {
			if (contains(t)) return map.get(t);
			int id = idStore.apply(t);
			list.add(t);
			map.put(t, id);
			assert id == list.size();
			return id;
		}

		public void remove(T t) {
			if (!contains(t)) return;
			remove(id(t));
		}

		public void remove(int id) {
			map.remove(get(id));
			list.set(id - 1, null);
		}

		public void remove(List<Integer> tokens) {
			tokens.forEach(this::remove);
		}

		public Stream<T> stream() {
			return list.stream().filter(Objects::nonNull);
		}

		public void set(int id, T t) {
			map.remove(get(id));
			map.put(t, id);
			list.set(id - 1, t);
		}
	}

	public interface SubjectFilter {
		SubjectFilter Empty = emptyQuery();

		SubjectFilter with(Token token);

		SubjectFilter without(Token token);

		default SubjectFilter with(String key, String value) {
			return with(new Token(key, value));
		}

		default SubjectFilter without(String key, String value) {
			return without(new Token(key, value));
		}

		Subjects all();

		Subjects roots();
	}

	public interface AttributeFilter {
		Subjects contains(String value);

		Subjects matches(String value);
	}

	private static SubjectFilter emptyQuery() {
		return new SubjectFilter() {

			@Override
			public SubjectFilter with(Token token) {
				return this;
			}

			@Override
			public SubjectFilter without(Token token) {
				return this;
			}

			@Override
			public Subjects roots() {
				return new Subjects(List.of());
			}

			@Override
			public Subjects all() {
				return roots();
			}
		};
	}
}

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

import static systems.intino.alexandria.datamarts.subjectmap.model.Subject.Null;

public class SubjectIndex implements Closeable {
	private final Registry registry;
	private final Lookup<Subject> subjects;
	private final Lookup<Token> tokens;

	public SubjectIndex(File file) {
		this(SqliteConnection.from(file));
	}

	public SubjectIndex() {
		this(SqliteConnection.inMemory());
	}

	public SubjectIndex(Connection connection) {
		this.registry = new SqlRegistry(connection);
		this.subjects = new Lookup<>(registry.subjects(), subject -> Subject.of(subject, this::children), this::insert);
		this.tokens = new Lookup<>(registry.tokens(), Token::deserialize, this::insert);
	}

	private List<Subject> children(Subject subject) {
		return subjects.stream()
				.filter(s -> s.parent().equals(subject))
				.toList();
	}

	public TokenQuery tokens() {
		return new TokenQuery() {
			public TokenSet of(Subject subject) {
				int id = subjects.id(subject);
				return new TokenSet(id < 0 ? List.of() : tokens(registry.tokensOf(id)).toList());
			}
		};
	}

	public SubjectQuery subjects(String type) {
		return new SubjectQuery() {

			@Override
			public SubjectSet all() {
				return subjectFilter(type).all();
			}

			@Override
			public SubjectSet roots() {
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
		return subjects.find(subject, Null);
	}

	public interface TokenQuery {
		default TokenSet of(String path) {
			return of(Subject.of(path));
		}
		TokenSet of(Subject subject);
	}

	public interface SubjectQuery {
		SubjectSet all();

		SubjectSet roots();
		AttributeFilter where(String... keys);
		SubjectFilter with(String user, String mail);
		SubjectFilter without(String description, String simulation);
	}

	private AttributeFilter attributeFilter(String type, Set<String> keys) {
		return new AttributeFilter() {
			@Override
			public SubjectSet contains(String value) {
				List<Integer> tokens = filterContaining(value);
				return subjectSetWith(tokens);
			}

			@Override
			public SubjectSet matches(String value) {
				List<Integer> tokens = filterFitting(value);
				return subjectSetWith(tokens);
			}

			private SubjectSet subjectSetWith(List<Integer> tokens) {
				List<Subject> subjects = tokens.isEmpty() ? List.of() : subjects(registry.subjectsFilteredBy(subjectsWith(type), tokens)).toList();
				return new SubjectSet(subjects);
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
			public SubjectSet all() {
				return calculate(s -> true);
			}

			@Override
			public SubjectSet roots() {
				return calculate(s -> s.parent().isNull());
			}

			private SubjectSet calculate(Predicate<Subject> predicate) {
				List<Integer> candidates = subjectsWith(type);
				List<Integer> search = registry.subjectsFilteredBy(candidates, condition);
				List<Subject> subjects = subjects(search).filter(predicate).toList();
				return new SubjectSet(subjects);
			}
		};
	}

	private List<Integer> subjectsWith(String type) {
		return subjects.stream()
				.filter(Objects::nonNull)
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
			private final int id = subjects.add(wrap(subject));

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
		return new Subject(subject.path(), this::children);
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
		return search.stream().map(subjects::get);
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
			return list.stream();
		}

		public void set(int id, T t) {
			map.remove(get(id));
			map.put(t, id);
			list.set(id - 1, t);
		}


		public T find(T t, T orElse) {
			int id = id(t);
			return id > 0 ? get(id) : orElse;
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

		SubjectSet all();

		SubjectSet roots();
	}

	public interface AttributeFilter {
		SubjectSet contains(String value);

		SubjectSet matches(String value);
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
			public SubjectSet roots() {
				return new SubjectSet(List.of());
			}

			@Override
			public SubjectSet all() {
				return roots();
			}
		};
	}
}

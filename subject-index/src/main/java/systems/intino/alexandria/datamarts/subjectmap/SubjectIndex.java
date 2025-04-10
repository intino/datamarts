package systems.intino.alexandria.datamarts.subjectmap;

import systems.intino.alexandria.datamarts.subjectmap.io.Registry;
import systems.intino.alexandria.datamarts.subjectmap.io.registries.SqlRegistry;
import systems.intino.alexandria.datamarts.subjectmap.io.registries.SqliteConnection;
import systems.intino.alexandria.datamarts.subjectmap.model.*;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

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
		this.subjects = new Lookup<>(registry.subjects(), Subject::deserialize, this::insert);
		this.tokens = new Lookup<>(registry.tokens(), Token::deserialize, this::insert);
	}

	public TokenQuery tokens() {
		return new TokenQuery() {
			public TokenSet of(Subject subject) {
				int id = subjects.id(subject);
				return new TokenSet(id < 0 ? List.of() : tokens(registry.tokensOf(id)));
			}
		};
	}

	public SubjectQuery subjects() {
		return new SubjectQuery() {
			@Override
			public SubjectSet toSet() {
				return subjectFilter().toSet();
			}

			@Override
			public AttributeFilter where(String key) {
				return attributeFilter(key);
			}

			@Override
			public SubjectFilter with(String key, String value) {
				return subjectFilter().with(key, value);
			}

			@Override
			public SubjectFilter without(String key, String value) {
				return subjectFilter().without(key, value);
			}
		};
	}

	public interface TokenQuery {
		TokenSet of(Subject subject);
		default TokenSet of(String subject, String type) {
			return of(new Subject(subject, type));
		}
	}

	public interface SubjectQuery {
		AttributeFilter where(String key);
		SubjectFilter with(String user, String mail);
		SubjectFilter without(String description, String simulation);
		SubjectSet toSet();
	}

	private AttributeFilter attributeFilter(String key) {
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
				List<Subject> subjects = tokens.isEmpty() ? List.of() : subjects(registry.subjectsFilteredBy(tokens));
				return new SubjectSet(subjects);
			}

			private List<Integer> filterFitting(String value) {
				return tokens(key)
						.filter(t -> pattern(t.value()).matcher(value).matches())
						.map(tokens::id)
						.toList();
			}

			private List<Integer> filterContaining(String value) {
				return tokens(key)
						.filter(t -> t.value().contains(value))
						.map(tokens::id)
						.toList();
			}

			private Stream<Token> tokens(String key) {
				return tokens.stream().filter(t -> t.key().equals(key));
			}
		};
	}

	private SubjectFilter subjectFilter() {
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
			public SubjectSet toSet() {
				List<Integer> search = registry.subjectsFilteredBy(condition);
				List<Subject> subjects = subjects(search);
				return new SubjectSet(subjects);
			}
		};
	}

	private final Map<String, Pattern> patterns = new HashMap<>();

	private Pattern pattern(String value) {
		return patterns.computeIfAbsent(value, s -> Pattern.compile(value));
	}


	public Indexing on(String subject, String type) {
		return on(new Subject(subject, type));
	}

	public Indexing on(Subject subject) {
		return new Indexing() {
			private final int id = subjects.add(subject);

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

	public void drop(String subject, String type) {
		drop(new Subject(subject, type));
	}

	public void drop(Subject subject) {
		if (!subjects.contains(subject)) return;
		registry.drop(subjects.id(subject));
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

	private List<Subject> subjects(List<Integer> search) {
		return search.stream().map(subjects::get).toList();
	}

	private List<Token> tokens(List<Integer> tokens) {
		return tokens.stream().map(this.tokens::get).toList();
	}

	public interface Indexing {
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
		private final Function<T, Integer> function;
		private final Map<T, Integer> map;

		public Lookup(List<String> list, Function<String,T> deserializer, Function<T, Integer> function) {
			this.list = new ArrayList<>(list.stream().map(deserializer).toList());
			this.function = function;
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
			int id = function.apply(t);
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

		public Stream<T> stream() {
			return list.stream();
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

		SubjectSet toSet();

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
				public SubjectSet toSet() {
					return new SubjectSet(List.of());
				}
			};
		}
	}

	public interface AttributeFilter {
		SubjectSet contains(String value);

		SubjectSet matches(String value);
	}
}

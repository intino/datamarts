package systems.intino.datamarts.subjectindex;

import systems.intino.datamarts.subjectindex.io.Registry;
import systems.intino.datamarts.subjectindex.io.StatementFeeder;
import systems.intino.datamarts.subjectindex.io.feeders.RawStatementFeeder;
import systems.intino.datamarts.subjectindex.io.registries.SqlRegistry;
import systems.intino.datamarts.subjectindex.io.registries.SqliteConnection;
import systems.intino.datamarts.subjectindex.model.*;
import systems.intino.datamarts.subjectindex.model.Subject.Transaction;

import java.io.*;
import java.sql.Connection;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static systems.intino.datamarts.subjectindex.model.Subject.Any;

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
		this.subjects = new Lookup<>(registry.subjects(), Subject::of, this::insert);
		this.tokens = new Lookup<>(registry.tokens(), Token::of, this::insert);
	}

	public Subject get(String name, String type) {
		return get(new Subject(name, type));
	}

	public Subject get(String path) {
		return get(Subject.of(path));
	}

	private Subject get(Subject subject) {
		return subjects.contains(subject) ? wrap(subject) : null;
	}

	private Subject get(int subject) {
		return subjects.contains(subject) ? wrap(subjects.get(subject)) : null;
	}

	public Tokens tokens() {
		return new Tokens(tokens.stream().toList());
	}

	public SubjectQuery subjects() {
		return subjects(Any);
	}

	public SubjectQuery subjects(String... types) {
		return subjects(setOf(types));
	}

	public SubjectQuery subjects(Set<String> types) {
		return new SubjectQuery() {

			@Override
			public Subjects all() {
				return subjectFilter(types).all();
			}

			@Override
			public Subjects roots() {
				return subjectFilter(types).roots();
			}

			@Override
			public AttributeFilter where(String... keys) {
				return attributeFilter(types, Set.of(keys));
			}

			@Override
			public SubjectFilter with(String key, String value) {
				return subjectFilter(types).with(key, value);
			}

			@Override
			public SubjectFilter without(String key, String value) {
				return subjectFilter(types).without(key, value);
			}
		};	}

	private Set<String> setOf(String[] types) {
		return Set.of(types);
	}

	public Subject create(String name, String type) {
		return create(new Subject(name, type));
	}

	public Subject create(String path) {
		return create(Subject.of(path));
	}

	private Subject create(Subject subject) {
		int id = subjects.add(subject);
		registry.commit();
		return get(id);
	}

	private Transaction update(Subject subject) {
		return new Transaction() {
			private final int id = subjects.id(subject);
			private final List<Integer> currentTokens = registry.tokensOf(id);

			@Override
			public Transaction rename(String identifier) {
				if (identifier == null || identifier.isEmpty()) return this;
				return replace(subject.identifier(identifier));
			}

			@Override
			public Transaction set(Token token) {
				del(token.key());
				put(token);
				return this;
			}

			public Transaction put(Token token) {
				registry.link(id, tokens.add(token));
				return this;
			}

			@Override
			public Transaction del(Token token) {
				registry.unlink(id, tokens.add(token));
				return this;
			}

			@Override
			public Transaction del(String key) {
				toTokens(currentTokens).filter(t -> t.is(key)).forEach(this::del);
				return this;
			}

			private Transaction replace(Subject subject) {
				if (subjects.contains(subject)) return this;
				subjects.set(id, subject);
				registry.rename(id, subject.toString());
				return this;
			}

			@Override
			public void commit() {
				registry.commit();
			}
		};
	}

	private void drop(Subject subject) {
		if (subject.isNull() || !subjects.contains(subject)) return;
		subject.children().forEach(this::drop);
		int id = subjects.id(subject);
		tokens.remove(registry.exclusiveTokensOf(id));
		registry.drop(id);
		subjects.remove(subject);
	}

	private SubjectFilter subjectFilter(Set<String> types) {
		return new SubjectFilter() {
			private final List<Integer> candidates = subjectsWith(types);
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
				List<Integer> search = condition.isEmpty() ? candidates : registry.subjectsFilteredBy(candidates, condition);
				List<Subject> subjects = toSubjects(search).filter(predicate).toList();
				return new Subjects(subjects);
			}
		};
	}

	private AttributeFilter attributeFilter(Set<String> types, Set<String> keys) {
		return new AttributeFilter() {
			private final List<Integer> candidates = subjectsWith(types);

			@Override
			public Subjects contains(String value) {
				String[] values = value.split(" ");
				return subjectSetWith(tokensWith(values));
			}

			private List<Integer> tokensWith(String[] values) {
				List<List<Integer>> lists = Arrays.stream(values)
						.filter(v -> !v.isEmpty())
						.map(this::tokensContaining)
						.collect(Collectors.toList());
				return join(lists);
			}

			private static List<Integer> join(List<List<Integer>> lists) {
				if (lists.isEmpty()) return List.of();
				Set<Integer> join = new HashSet<>(lists.getFirst());
				IntStream.range(1, lists.size())
						.mapToObj(lists::get)
						.forEach(join::retainAll);
				return new ArrayList<>(join);
			}
			@Override
			public Subjects matches(String value) {
				List<Integer> tokens = tokensFitting(value);
				return subjectSetWith(tokens);
			}

			private Subjects subjectSetWith(List<Integer> tokens) {
				List<Subject> subjects = tokens.isEmpty() ? List.of() : toSubjects(registry.subjectsFilteredBy(candidates, tokens)).toList();
				return new Subjects(subjects);
			}

			private List<Integer> tokensFitting(String value) {
				return tokens(keys)
						.filter(t -> pattern(t.value()).matcher(value).matches())
						.map(tokens::id)
						.toList();
			}

			private List<Integer> tokensContaining(String value) {
				return tokens(keys)
						.filter(t -> t.value().contains(value))
						.map(tokens::id)
						.toList();
			}

			private Stream<Token> tokens(Set<String> keys) {
				return tokens.stream()
						.filter(t -> keys.contains(t.key()));
			}
		};
	}

	public static final double nullThresholdRatio = 0.20;
	public boolean isFragmented() {
		return subjects.nullRatio() > nullThresholdRatio || tokens.nullRatio() > nullThresholdRatio;
	}

	public StatementFeeder statementFeeder() {
		return this::stamentIterator;
	}

	public SubjectIndex consume(StatementFeeder statementFeeder) {
		Batch batch = batch();
		for (Statement statement : statementFeeder)
			batch.register(statement);
		batch.commit();
		return this;
	}

	public void copyTo(SubjectIndex subjectIndex) {
		subjectIndex.consume(this.statementFeeder());
	}

	public void dump(OutputStream os) {
		registry.dump().forEach(s->write(s + '\n', os));
	}

	public SubjectIndex restore(InputStream is) {
		return consume(new RawStatementFeeder(is));
	}

	private Iterator<Statement> stamentIterator() {
		return registry.dump().map(Statement::new).iterator();
	}

	private void write(String str, OutputStream os) {
		try {
			os.write(str.getBytes());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		registry.close();
	}

	private Subject.Context context() {
		return new Subject.Context() {
			@Override
			public Subjects children(Subject subject) {
				return new Subjects(subjects.stream()
						.filter(s -> s.parent().equals(subject))
						.map(s->wrap(s))
						.toList());
			}

			@Override
			public Tokens tokens(Subject subject) {
				int id = subjects.id(subject);
				return new Tokens(id < 0 ? List.of() : toTokens(registry.tokensOf(id)).toList());
			}

			@Override
			public Subject create(Subject subject) {
				return SubjectIndex.this.create(subject.path());
			}

			@Override
			public Transaction update(Subject subject) {
				return SubjectIndex.this.update(subject);
			}

			@Override
			public void drop(Subject subject) {
				SubjectIndex.this.drop(subject);
			}
		};
	}

	private List<Integer> subjectsWith(Set<String> types) {
		return subjectsWith(predicateFor(types));
	}

	private List<Integer> subjectsWith(Predicate<Subject> predicate) {
		return subjects.stream()
				.filter(predicate)
				.map(subjects::id)
				.toList();
	}

	private Predicate<Subject> predicateFor(Set<String> types) {
		return types.contains(Any) ? s->true : s -> types.contains(s.type());
	}

	private final Map<String, Pattern> patterns = new HashMap<>();

	private Pattern pattern(String value) {
		return patterns.computeIfAbsent(value, s -> Pattern.compile(value));
	}

	private Subject wrap(Subject subject) {
		return new Subject(subject, context());
	}

	private int insert(Subject subject) {
		return registry.insertSubject(subject.path());
	}

	private int insert(Token token) {
		return registry.insertToken(token.toString());
	}

	private Stream<Subject> toSubjects(List<Integer> subjects) {
		return subjects.stream()
				.map(this.subjects::get)
				.map(this::wrap);
	}

	private Stream<Token> toTokens(List<Integer> tokens) {
		return tokens.stream().map(this.tokens::get);
	}

	public Batch batch() {
		return new Batch() {
			@Override
			public void register(Subject subject, Token token) {
				int subjectId = subjects.add(subject);
				int tokenId = tokens.add(token);
				registry.link(subjectId, tokenId);
			}

			@Override
			public void commit() {
				registry.commit();
			}
		};
	}

	public interface Batch {

		default void register(Statement statement) {
			register(statement.subject(), statement.token());
		}

		void register(Subject subject, Token token);
		void commit();
	}

	public interface SubjectQuery {
		Subjects all();
		Subjects roots();
		SubjectFilter with(String key, String value);
		SubjectFilter without(String key, String value);
		AttributeFilter where(String... keys);
	}

	public interface SubjectFilter {
		Subjects all();
		Subjects roots();

		SubjectFilter Empty = emptyQuery();
		SubjectFilter with(Token token);
		SubjectFilter without(Token token);

		default SubjectFilter with(String key, String value) {
			return with(new Token(key, value));
		}
		default SubjectFilter without(String key, String value) {
			return without(new Token(key, value));
		}
	}

	public interface AttributeFilter {
		Subjects contains(String value);
		Subjects matches(String value);
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

		public boolean contains(int id) {
			return id <= list.size();
		}

		public double nullRatio() {
			return list.size() > 20 ? (double) nullItems() / list.size() : 0;
		}

		private long nullItems() {
			return list.stream().filter(Objects::isNull).count();
		}
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

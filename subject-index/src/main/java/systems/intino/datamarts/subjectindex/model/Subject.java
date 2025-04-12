package systems.intino.datamarts.subjectindex.model;

import java.util.List;
import java.util.Objects;

public record Subject(String path, Context context) {
	public static final String Any = "*";
	public static final Context Null = nullContext();

	public Subject {
		path = path != null ? path.trim() : "";
		context = context != null ? context : Null;
	}

	public Subject(Subject subject, Context context) {
		this(subject.path, context);
	}

	public static Subject of(String path) {
		return path != null ? new Subject(path, null) : null;
	}

	public static Subject of(String name, String type) {
		return of(name + "." + type);
	}

	public static Subject of(Subject subject, String name, String type) {
		return of(subject, name + "." + type);
	}

	public static Subject of(Subject subject, String identifier) {
		return of(subject.path() + "/" + identifier);
	}

	public String identifier() {
		int i = path.lastIndexOf('/');
		return path.substring(i + 1).trim();
	}

	public Subject identifier(String identifier) {
		String parentPath = parentPath();
		return parentPath.isEmpty() ?
				new Subject(identifier, context) :
				new Subject(parentPath + "/" + identifier, context);
	}

	public String name() {
		String identifier = identifier();
		int i = identifier.lastIndexOf('.');
		return i >= 0 ? identifier.substring(0, i) : identifier;
	}

	public String type() {
		String identifier = identifier();
		int i = identifier.lastIndexOf('.');
		return i >= 0 ? identifier.substring(i + 1) : "";
	}

	public Subject parent() {
		return new Subject(parentPath(), context);
	}

	public Subjects children() {
		checkIfContextExists();
		return context.children(this);
	}

	public Tokens tokens() {
		checkIfContextExists();
		return context.tokens(this);
	}

	public Subject create(String name, String type) {
		Subject child = Subject.of(this, name, type);
		return context != null ? context.create(new Subject(child, context)) : child;
	}

	public Transaction update() {
		checkIfContextExists();
		return context.update(this);
	}

	public void drop() {
		checkIfContextExists();
		context.drop(this);
	}

	private void checkIfContextExists() {
		if (context == Null)
			System.err.println("Context is not defined for '" + path + "'");
	}

	public boolean is(String type) {
		return type.equals("*") || type.equals(this.type());
	}

	public boolean isNull() {
		return this.path.isEmpty();
	}

	@Override
	public String toString() {
		return path;
	}

	@Override
	public boolean equals(Object object) {
		return object instanceof Subject subject && Objects.equals(path, subject.path);
	}

	@Override
	public int hashCode() {
		return Objects.hash(path);
	}

	private String parentPath() {
		int i = path.lastIndexOf('/');
		return i >= 0 ? path.substring(0, i) : "";
	}

	public boolean isRoot() {
		return parent().isNull();
	}


	public interface Context {
		Subjects children(Subject subject);
		Tokens tokens(Subject subject);

		Subject create(Subject child);
		Transaction update(Subject subject);
		void drop(Subject subject);
	}

	public interface Transaction {
		Transaction rename(String identifier);

		Transaction set(Token token);
		Transaction put(Token token);
		Transaction del(Token token);
		Transaction del(String key);

		default Transaction set(String key, String value) {
			return set(new Token(key, value));
		}
		default Transaction put(String key, String value) {
			return put(new Token(key, value));
		}
		default Transaction del(String key, String value) {
			return del(new Token(key, value));
		}

		void commit();
	}

	private static Context nullContext() {
		return new Context() {
			@Override
			public Subjects children(Subject subject) {
				return new Subjects(List.of());
			}

			@Override
			public Tokens tokens(Subject subject) {
				return new Tokens(List.of());
			}

			@Override
			public Transaction update(Subject subject) {
				return null;
			}

			@Override
			public Subject create(Subject child) {
				return child;
			}

			@Override
			public void drop(Subject subject) {

			}
		};
	}
}

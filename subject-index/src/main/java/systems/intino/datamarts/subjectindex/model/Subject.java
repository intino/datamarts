package systems.intino.datamarts.subjectindex.model;

import java.util.List;
import java.util.Objects;

import static systems.intino.datamarts.subjectindex.model.Subject.Context.Null;

public record Subject(String path, Context context) {
	public static final String Any = "*";

	public static Subject of(String path) {
		if (path == null) return null;
		return new Subject(path);
	}

	public Subject {
		path = path != null ? path.trim() : "";
		context = context != null ? context : Null;
	}

	public Subject(Subject subject, Context context) {
		this(subject.path, context);
	}

	public Subject(String path) {
		this(path, Null);
	}

	public Subject(String name, String type) {
		this(name + "." + type, Null);
	}

	public Subject(Subject parent, String identifier) {
		this(parent.path() + "/" + identifier, parent.context());
	}

	public Subject(Subject parent, String name, String type) {
		this(parent, name + "." + type);
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
		Subject child = new Subject(this, name, type);
		return context != null ? context.create(child) : child;
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
		if (context != Null) return;
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
		Context Null = nullContext();
		Subjects children(Subject subject);
		Tokens tokens(Subject subject);

		Subject create(Subject child);
		Transaction update(Subject subject);
		void drop(Subject subject);
	}

	public interface Transaction {
		Transaction Null = nullTransaction();


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
				return Transaction.Null;
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

	private static Transaction nullTransaction() {
		return new Transaction() {
			@Override
			public Transaction rename(String identifier) {
				return this;
			}

			@Override
			public Transaction set(Token token) {
				return this;
			}

			@Override
			public Transaction put(Token token) {
				return this;
			}

			@Override
			public Transaction del(Token token) {
				return this;
			}

			@Override
			public Transaction del(String key) {
				return this;
			}

			@Override
			public void commit() {

			}
		};
	}

}

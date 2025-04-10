package systems.intino.alexandria.datamarts.subjectmap.model;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public record Subject(String path, Context context) {
	public static final String Any = "*";

	public Subject {
		path = path != null ? path.trim() : "";
	}

	public static Subject of(String path, Context context) {
		return new Subject(path, context);
	}

	public static Subject of(String path) {
		return of(path, Context.Null);
	}

	public String identifier() {
		int i = path.lastIndexOf('/');
		return path.substring(i + 1).trim();
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

	public Subject identifier(String identifier) {
		String parentPath = parentPath();
		return parentPath.isEmpty() ?
				new Subject(identifier, context) :
				new Subject(parentPath + "/" + identifier, context);
	}

	public Subjects children() {
		checkContext();
		return context.children(this);
	}

	public Tokens tokens() {
		checkContext();
		return context.tokens(this);
	}

	private void checkContext() {
		if (context != Context.Null) return;
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

	public record Context(Function<Subject, Subjects> childrenLookUp, Function<Subject, Tokens> tokenLookUp) {
		public static final Context Null = new Context(s->new Subjects(List.of()), s->new Tokens(List.of()));

		public Subjects children(Subject subject) {
			return childrenLookUp.apply(subject);
		}

		public Tokens tokens(Subject subject) {
			return tokenLookUp.apply(subject);
		}
	}
}

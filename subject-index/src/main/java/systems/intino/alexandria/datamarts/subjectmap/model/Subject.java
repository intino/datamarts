package systems.intino.alexandria.datamarts.subjectmap.model;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public record Subject(String path, Function<Subject, List<Subject>> childrenLookup) {
	public static final String Any = "*";
	public static final Subject Null = new Subject("", s->List.of());

	public static Subject of(String path, Function<Subject, List<Subject>> childrenLookup) {
		return new Subject(path, childrenLookup);
	}

	public static Subject of(String path) {
		return of(path, s -> List.of());
	}

	public String identifier() {
		int i = path.lastIndexOf('/');
		return path.substring(i + 1);
	}

	public String name() {
		String identifier = identifier();
		int i = identifier.lastIndexOf('.');
		return identifier.substring(0, i);
	}

	public String type() {
		String identifier = identifier();
		int i = identifier.lastIndexOf('.');
		return identifier.substring(i + 1);
	}

	public Subject parent() {
		return new Subject(parentPath(), childrenLookup);
	}

	public Subject identifier(String identifier) {
		String parentPath = parentPath();
		return parentPath.isEmpty() ?
				new Subject(identifier, childrenLookup) :
				new Subject(parentPath + "/" + identifier, childrenLookup);
	}

	public List<Subject> children() {
		return childrenLookup.apply(this);
	}

	public boolean is(String type) {
		return type.equals("*") || this.type().equals(type);
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
}

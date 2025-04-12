package systems.intino.datamarts.subjectindex.model;

import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public record Tokens(List<Token> items) implements Iterable<Token> {
	public boolean isEmpty() {
		return items.isEmpty();
	}

	public int size() {
		return items.size();
	}

	public Token get(int index) {
		return items.get(index);
	}

	public Values get(String name) {
		return get(values(t->t.key().equals(name)));
	}

	private Values get(List<String> values) {
		return new Values() {
			@Override
			public String first() {
				return values.getFirst();
			}

			@Override
			public String serialize() {
				return values.stream().map(Object::toString).collect(Collectors.joining("\n"));
			}

			@Override
			public Iterator<String> iterator() {
				return values.iterator();
			}
		};
	}

	private List<String> values(Predicate<Token> predicate) {
		return items.stream().filter(predicate).map(Token::value).toList();
	}

	public Tokens filter(Predicate<Token> predicate) {
		return new Tokens(items.stream().filter(predicate).collect(Collectors.toList()));
	}

	@Override
	public Iterator<Token> iterator() {
		return items.iterator();
	}

	public Stream<Token> stream() {
		return items.stream();
	}

	public String serialize() {
		return items.stream()
				.map(Token::toString)
				.collect(Collectors.joining("\n"));
	}

	public interface Values extends Iterable<String> {
		String first();

		String serialize();
	}
}

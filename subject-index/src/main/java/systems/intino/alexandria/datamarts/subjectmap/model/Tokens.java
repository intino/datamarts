package systems.intino.alexandria.datamarts.subjectmap.model;

import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public record Tokens(List<Token> tokens) implements Iterable<Token> {
	public boolean isEmpty() {
		return tokens.isEmpty();
	}

	public int size() {
		return tokens.size();
	}

	public Token get(int index) {
		return tokens.get(index);
	}

	public Values get(String name) {
		return get(filter(t->t.key().equals(name)));
	}

	private Values get(List<String> values) {
		return new Values() {
			@Override
			public String first() {
				return values.getFirst();
			}

			@Override
			public Iterator<String> iterator() {
				return values.iterator();
			}
		};
	}

	private List<String> filter(Predicate<Token> predicate) {
		return tokens.stream().filter(predicate).map(Token::value).toList();
	}

	@Override
	public Iterator<Token> iterator() {
		return tokens.iterator();
	}

	public Stream<Token> stream() {
		return tokens.stream();
	}

	public String serialize() {
		return tokens.stream()
				.map(Token::toString)
				.collect(Collectors.joining("\n"));
	}

	public interface Values extends Iterable<String> {
		String first();
	}
}

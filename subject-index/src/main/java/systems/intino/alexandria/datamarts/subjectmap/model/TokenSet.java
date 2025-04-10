package systems.intino.alexandria.datamarts.subjectmap.model;

import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public record TokenSet(List<Token> tokens) implements Iterable<Token> {
	public String serialize() {
		return tokens.stream().map(Token::toString).collect(Collectors.joining("\n"));
	}

	public boolean isEmpty() {
		return tokens.isEmpty();
	}

	public int size() {
		return tokens.size();
	}

	public Token get() {
		return tokens.getFirst();
	}

	public Token get(int index) {
		return tokens.get(index);
	}

	public TokenSet filter(Predicate<Token> predicate) {
		return new TokenSet(tokens.stream().filter(predicate).toList());
	}

	@Override
	public Iterator<Token> iterator() {
		return tokens.iterator();
	}
}

package systems.intino.alexandria.datamarts.calculator.parser;

public record Token(Type type, int startPosition, String value) {
	public enum Type {
		BRACE_OPEN, BRACE_CLOSE,
		NUMBER, IDENTIFIER,
		COMMA, OPERATOR,
		UNKNOWN
	}
}

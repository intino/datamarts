package systems.intino.alexandria.datamarts.subjectstore.calculator.parser;

public record Token(Type type, int startPosition, String value) {
	public enum Type {
		BRACE_OPEN, BRACE_CLOSE,
		NUMBER, IDENTIFIER, OPERATOR,
		UNKNOWN
	}
}

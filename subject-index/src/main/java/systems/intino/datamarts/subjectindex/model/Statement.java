package systems.intino.datamarts.subjectindex.model;

public record Statement(Subject subject, Token token) {

	public Statement(Subject subject, String key, String value) {
		this(subject, new Token(key, value));
	}

	public Statement(String subject, String token) {
		this(Subject.of(subject), Token.of(token));
	}

	public Statement(String str) {
		this(str.split("\t"));
	}

	private Statement(String[] split) {
		this(split[0], split[1]);
	}

	@Override
	public String toString() {
		return subject + "\t" + token;
	}
}

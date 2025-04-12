package systems.intino.datamarts.subjectindex.model;

public record Statement(Subject subject, Token token) {

	public Statement(Subject subject, String key, String value) {
		this(subject, new Token(key, value));
	}

	public Statement(String subject, String key, String value) {
		this(Subject.of(subject), new Token(key, value));
	}

	@Override
	public String toString() {
		return subject + "\t" + token;
	}
}

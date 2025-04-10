package systems.intino.alexandria.datamarts.subjectmap.model;

public record Token(String key, String value) {

	public Token {
		key = key.trim();
		value = value.trim();
	}

	public static Token of(String str) {
		if (str == null || str.isEmpty()) return null;
		String[] split = str.split("=");
		return new Token(split[0], split[1]);
	}

	@Override
	public String toString() {
		return key + "=" + value;
	}

}

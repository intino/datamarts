package systems.intino.datamarts.subjectindex.model;

public record Token(String key, String value) {

	public static Token of(String str) {
		if (str == null || str.isEmpty()) return null;
		String[] split = str.split("=",2);
		return new Token(split[0], split[1]);
	}

	public Token {
		key = key.trim();
		value = value.trim();
	}

	@Override
	public String toString() {
		return key + "=" + value;
	}

	public boolean is(String key) {
		return key.equals(this.key);
	}
}

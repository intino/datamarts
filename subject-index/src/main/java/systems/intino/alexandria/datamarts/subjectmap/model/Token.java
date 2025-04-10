package systems.intino.alexandria.datamarts.subjectmap.model;

public record Token(String key, String value) {

	public static Token deserialize(String str) {
		String[] split = str.split("=");
		return new Token(split[0], split[1]);
	}

	@Override
	public String toString() {
		return key + "=" + value;
	}

}

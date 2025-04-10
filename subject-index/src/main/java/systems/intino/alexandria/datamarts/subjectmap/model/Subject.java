package systems.intino.alexandria.datamarts.subjectmap.model;

public record Subject(String name, String type) {

	public static Subject deserialize(String str) {
		if (str == null) return null;
		String[] split = str.split(":");
		return new Subject(split[0], split[1]);
	}

	@Override
	public String toString() {
		return name + ":" + type;
	}

	public boolean is(String type) {
		return this.type.equals(type);
	}

}

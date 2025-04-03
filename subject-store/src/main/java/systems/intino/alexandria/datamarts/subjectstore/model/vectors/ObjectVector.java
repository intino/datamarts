package systems.intino.alexandria.datamarts.subjectstore.model.vectors;

import systems.intino.alexandria.datamarts.subjectstore.model.Vector;

import java.util.Arrays;

public record ObjectVector(Object[] values) implements Vector<Object> {
	@Override
	public Object get(int index) {
		return values[index];
	}

	@Override
	public int size() {
		return values.length;
	}

	@Override
	public String toString() {
		return Arrays.toString(values);
	}
}

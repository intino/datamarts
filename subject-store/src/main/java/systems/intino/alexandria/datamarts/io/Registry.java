package systems.intino.alexandria.datamarts.io;

import systems.intino.alexandria.datamarts.model.Bundle;

import java.io.Closeable;
import java.io.OutputStream;
import java.util.Map;

public interface Registry extends Closeable {

	int size();

	default boolean isEmpty() { return size() == 0; }

	Bundle tags();

	Bundle instants();

	String ss(int feed);

	double getNumber(int tag, int feed);

	String getText(int tag, int feed);

	Bundle getNumbers(int tag, int from, int to);

	Bundle getTexts(int tag, int from, int to);

	void setTag(int id, String label);

	void setTagLastFeed(int id, int feed);

	int nextFeed();

	void put(int tag, Object value);

	void push();

	void dump(OutputStream os, Map<Object, String> dictionary);
}
